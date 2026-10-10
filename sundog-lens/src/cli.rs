//! The command line: a hand-rolled parser with no external dependency.
//!
//! [`parse`] is pure. It maps the arguments after `argv[0]` to a [`Command`]
//! or a [`CliError`]; the binary prints the error and exits 2.

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroU8;
use std::path::PathBuf;
use std::time::Duration;

use crate::key::KeySpec;
use crate::ui::theme::ColorChoice;

/// The help text.
pub const HELP: &str = "\
sundog-lens: watch a sundog cluster from outside

USAGE
  sundog-lens [watch] <cluster> [--seed HOST:PORT]... [--bind IP:PORT] [--advertise IP]
              [--metrics TEMPLATE]... [--scrape NODE=URL]... [--interval 1s]
              [--forget-after 90s] [--color auto|truecolor|256|mono] [--no-bg]
              [--no-braille] [--no-anim] [--exit-after DUR] [--log FILE]
              [--once [--json] [--settle 3s] [--explain KEY [--cache NAME]]]
  sundog-lens cluster [--name lens-demo] [--nodes 3] [--base-ip IP]
              [--testnode PATH] [--owners 2] [--keys 20000] [--rate 1500] [--logs DIR]
  sundog-lens demo [--scenario tour|FILE] [--headless] [--no-captions] [--marks FILE]
              [--log FILE] [cluster flags] [--color ...] [--no-bg] [--no-braille]
              [--no-anim]

watch    Join the cluster's gossip as an observer and show its members, caches and
         part ownership. The observer opens no cache and is never a peer.
cluster  Start a local fleet of sundog-testnode processes under load.
demo     Run the fleet, a scripted scenario and the interface in one process.

OPTIONS
  --seed HOST:PORT      A gossip address to join through; repeatable. Without it the
                        observer reads SUNDOG_SEEDS, then falls back to mDNS.
  --bind IP:PORT        The observer's gossip bind address (default 0.0.0.0:0).
  --advertise IP        The IP the observer advertises.
  --metrics TEMPLATE    A scrape URL with {ip}, {gossip_port}, {data_port}, {node_id},
                        {gossip_port+N} and {gossip_port-N}; repeatable.
  --base-ip IP          With cluster and demo: give each node its own loopback address,
                        the first node at IP and the rest counting up from it, all on
                        the fixed ports (gossip 7946, control 8080, exporter 9090).
                        Without it, Linux does this from 127.0.0.11, and any other
                        system runs every node on 127.0.0.1 with the ports counting
                        up from those: n1 on 7946, n2 on 7947, and so on.
  --scrape NODE=URL     Pin one node's exporter URL; NODE is a label, a node-id hex
                        prefix or ip:port; repeatable.
  --interval DUR        The scrape interval (default 1s).
  --forget-after DUR    Hide a gone node after this long (default 90s).
  --color MODE          auto, truecolor, 256 or mono (default auto).
  --no-bg               Do not paint the background.
  --no-braille          Draw block characters instead of braille.
  --no-anim             Turn motion off.
  --exit-after DUR      Quit after this long.
  --log FILE            Write tracing output to FILE. With demo, the file records
                        failed keys, failed steps and timed-out awaits.
  --once                Print one report and exit.
  --json                With --once, print the report as JSON.
  --settle DUR          With --once, wait until the members hold still this long
                        (default 3s).
  --explain KEY         With --once, name KEY's part and its owners in fetch order,
                        computed from gossip after waiting up to 8 s for the view to
                        settle (the report says whether it did). Bare text is a
                        String key; uint:N, int:N, hex:BYTES (postcard bytes) and
                        str:TEXT (a String that starts with a prefix) select another
                        encoding.
  --cache NAME          With --explain, the Distributed cache; needed when several are.
  -h, --help            Print this help.

Durations take ms, s, m or h: 500ms, 2s, 1m.
";

/// What the command line asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Print [`HELP`].
    Help,
    /// `watch`, the default.
    Watch(WatchArgs),
    /// `cluster`.
    Cluster(FleetArgs),
    /// `demo`.
    Demo(DemoArgs),
}

/// A seed address: a literal socket address or a host name to resolve.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Seed {
    /// A literal `ip:port`.
    Addr(SocketAddr),
    /// A host name and port, resolved when the observer starts.
    Host(String, u16),
}

impl Seed {
    /// Parses `HOST:PORT`, where HOST is an IP address or a name.
    ///
    /// # Errors
    ///
    /// Returns a message when the port is missing or not a number, or the host
    /// is empty.
    pub fn parse(text: &str) -> Result<Self, String> {
        if let Ok(addr) = text.parse::<SocketAddr>() {
            return Ok(Self::Addr(addr));
        }
        let (host, port) = text
            .rsplit_once(':')
            .ok_or_else(|| "expected HOST:PORT".to_owned())?;
        if host.is_empty() || host.contains(':') {
            return Err("expected HOST:PORT".to_owned());
        }
        let port = port
            .parse()
            .map_err(|_| "the port must be a number from 0 to 65535".to_owned())?;
        Ok(Self::Host(host.to_owned(), port))
    }
}

impl fmt::Display for Seed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Addr(addr) => write!(f, "{addr}"),
            Self::Host(host, port) => write!(f, "{host}:{port}"),
        }
    }
}

/// One `--scrape NODE=URL` pin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScrapePin {
    /// A slot label, a node-id hex prefix or `ip:port`.
    pub node: String,
    /// The exporter URL.
    pub url: String,
}

/// Display flags shared by `watch` and `demo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DisplayArgs {
    /// `--color`.
    pub color: ColorChoice,
    /// `--no-bg`: leave the terminal background alone.
    pub no_bg: bool,
    /// `--no-braille`: block characters instead of braille.
    pub no_braille: bool,
    /// `--no-anim`: no motion.
    pub no_anim: bool,
}

impl Default for DisplayArgs {
    fn default() -> Self {
        Self {
            color: ColorChoice::Auto,
            no_bg: false,
            no_braille: false,
            no_anim: false,
        }
    }
}

/// The `--once` options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OnceArgs {
    /// `--json`: print one JSON object rather than text.
    pub json: bool,
    /// `--settle`: how long the member set must hold still.
    pub settle: Duration,
    /// `--explain`, with `--cache`: where one key lives.
    pub explain: Option<ExplainArgs>,
}

/// The `--explain` options of `--once`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainArgs {
    /// `--explain`: the key, read as [`KeySpec::parse`] reads it.
    pub key: KeySpec,
    /// `--cache`: the `Distributed` cache the key lives in; `None` lets the
    /// one ranked `Distributed` cache stand in.
    pub cache: Option<String>,
}

/// Arguments of `watch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchArgs {
    /// The cluster name.
    pub cluster: String,
    /// `--seed`, in order.
    pub seeds: Vec<Seed>,
    /// `--bind`.
    pub bind: SocketAddr,
    /// `--advertise`.
    pub advertise: Option<IpAddr>,
    /// `--metrics` templates, in order.
    pub metrics: Vec<String>,
    /// `--scrape` pins, in order.
    pub scrape: Vec<ScrapePin>,
    /// `--interval`.
    pub interval: Duration,
    /// `--forget-after`.
    pub forget_after: Duration,
    /// Display flags.
    pub display: DisplayArgs,
    /// `--exit-after`.
    pub exit_after: Option<Duration>,
    /// `--log`.
    pub log: Option<PathBuf>,
    /// `--once`, with its options.
    pub once: Option<OnceArgs>,
}

/// Fleet flags of `cluster` and `demo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FleetArgs {
    /// `--name`: the cluster name.
    pub name: String,
    /// `--nodes`.
    pub nodes: usize,
    /// `--base-ip`: slot 1's IP; slot `i` binds the `i`th address from it.
    /// `None` leaves the layout to the platform.
    pub base_ip: Option<Ipv4Addr>,
    /// `--testnode`: the `sundog-testnode` binary.
    pub testnode: Option<PathBuf>,
    /// `--owners`: owners per part.
    pub owners: NonZeroU8,
    /// `--keys`: the key-space size to fill.
    pub keys: u64,
    /// `--rate`: base operations per second per node.
    pub rate: u64,
    /// `--logs`: the directory for node logs.
    pub logs: Option<PathBuf>,
}

impl Default for FleetArgs {
    fn default() -> Self {
        Self {
            name: "lens-demo".to_owned(),
            nodes: 3,
            base_ip: None,
            testnode: None,
            owners: NonZeroU8::new(2).expect("2 is nonzero"),
            keys: 20_000,
            rate: 1_500,
            logs: None,
        }
    }
}

/// Which scenario `demo` plays.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScenarioSource {
    /// The built-in tour.
    Tour,
    /// A scenario file.
    File(PathBuf),
}

/// Arguments of `demo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DemoArgs {
    /// `--scenario`.
    pub scenario: ScenarioSource,
    /// `--headless`: print steps and events instead of drawing.
    pub headless: bool,
    /// `--no-captions`.
    pub no_captions: bool,
    /// `--marks`: write step times to this file.
    pub marks: Option<PathBuf>,
    /// `--log`: write tracing output to this file.
    pub log: Option<PathBuf>,
    /// The fleet flags.
    pub fleet: FleetArgs,
    /// The display flags.
    pub display: DisplayArgs,
}

/// Why the arguments did not parse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CliError {
    /// A flag the command does not take.
    UnknownFlag(String),
    /// A flag that needs a value has none.
    MissingValue(String),
    /// A flag's value is wrong.
    BadValue {
        /// The flag.
        flag: String,
        /// The value given.
        value: String,
        /// What is wrong with it.
        reason: String,
    },
    /// `watch` needs a cluster name.
    MissingCluster,
    /// A second positional argument.
    UnexpectedArg(String),
    /// A flag that needs another flag.
    Requires {
        /// The flag given.
        flag: &'static str,
        /// The flag it needs.
        needs: &'static str,
    },
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownFlag(flag) => write!(f, "unknown option {flag}"),
            Self::MissingValue(flag) => write!(f, "{flag} needs a value"),
            Self::BadValue {
                flag,
                value,
                reason,
            } => write!(f, "{flag}: bad value {value:?}: {reason}"),
            Self::MissingCluster => f.write_str("watch needs a cluster name"),
            Self::UnexpectedArg(arg) => write!(f, "unexpected argument {arg:?}"),
            Self::Requires { flag, needs } => write!(f, "{flag} needs {needs}"),
        }
    }
}

impl std::error::Error for CliError {}

/// Parses a duration written as an integer and a unit: `500ms`, `2s`, `1m`
/// or `1h`.
///
/// # Errors
///
/// Returns a message when the text has no unit, an unknown unit or a number
/// that does not fit.
pub fn parse_duration(text: &str) -> Result<Duration, String> {
    let split = text
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| "a duration needs a unit: ms, s, m or h".to_owned())?;
    let (digits, unit) = text.split_at(split);
    let amount: u64 = digits
        .parse()
        .map_err(|_| "a duration starts with a whole number".to_owned())?;
    let overflow = || "the duration is too large".to_owned();
    match unit {
        "ms" => Ok(Duration::from_millis(amount)),
        "s" => Ok(Duration::from_secs(amount)),
        "m" => amount
            .checked_mul(60)
            .map(Duration::from_secs)
            .ok_or_else(overflow),
        "h" => amount
            .checked_mul(3600)
            .map(Duration::from_secs)
            .ok_or_else(overflow),
        _ => Err(format!("unknown unit {unit:?}: use ms, s, m or h")),
    }
}

/// Parses the arguments after `argv[0]`.
///
/// A first argument of `watch`, `cluster` or `demo` selects that command;
/// anything else is the `watch` command, whose first positional argument is
/// the cluster name. `-h` or `--help` anywhere gives [`Command::Help`], and
/// so do no arguments at all.
///
/// # Errors
///
/// Returns a [`CliError`] for an unknown flag, a missing or bad value, a
/// missing cluster name, an extra positional argument or a flag whose
/// companion is absent.
pub fn parse<I, S>(args: I) -> Result<Command, CliError>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let args: Vec<String> = args.into_iter().map(Into::into).collect();
    if args.is_empty() || args.iter().any(|a| a == "-h" || a == "--help") {
        return Ok(Command::Help);
    }
    match args[0].as_str() {
        "watch" => parse_watch(&args[1..]).map(Command::Watch),
        "cluster" => parse_cluster(&args[1..]).map(Command::Cluster),
        "demo" => parse_demo(&args[1..]).map(Command::Demo),
        _ => parse_watch(&args).map(Command::Watch),
    }
}

/// A cursor over the arguments that resolves `--flag value` and
/// `--flag=value`.
struct Args<'a> {
    rest: &'a [String],
}

impl<'a> Args<'a> {
    fn new(rest: &'a [String]) -> Self {
        Self { rest }
    }

    /// The next argument as `(flag, inline value)`; a positional argument
    /// has no `--` prefix and returns as the flag with no value.
    fn next(&mut self) -> Option<(String, Option<String>)> {
        let (first, rest) = self.rest.split_first()?;
        self.rest = rest;
        if first.starts_with("--")
            && let Some((flag, value)) = first.split_once('=')
        {
            return Some((flag.to_owned(), Some(value.to_owned())));
        }
        Some((first.clone(), None))
    }

    /// The value of `flag`: its inline value, or the next argument.
    fn value(&mut self, flag: &str, inline: Option<&str>) -> Result<String, CliError> {
        if let Some(value) = inline {
            return Ok(value.to_owned());
        }
        let (value, rest) = self
            .rest
            .split_first()
            .ok_or_else(|| CliError::MissingValue(flag.to_owned()))?;
        self.rest = rest;
        Ok(value.clone())
    }
}

fn bad(flag: &str, value: &str, reason: impl Into<String>) -> CliError {
    CliError::BadValue {
        flag: flag.to_owned(),
        value: value.to_owned(),
        reason: reason.into(),
    }
}

/// A flag value parsed with `parse`, mapping its error to [`CliError`].
fn parsed<T, E: fmt::Display>(
    flag: &str,
    value: &str,
    parse: impl FnOnce(&str) -> Result<T, E>,
) -> Result<T, CliError> {
    parse(value).map_err(|e| bad(flag, value, e.to_string()))
}

/// A positive duration.
fn positive_duration(flag: &str, value: &str) -> Result<Duration, CliError> {
    let duration = parsed(flag, value, parse_duration)?;
    if duration.is_zero() {
        return Err(bad(flag, value, "must be greater than zero"));
    }
    Ok(duration)
}

/// Applies a display flag to `display`. Returns whether `flag` was one.
fn display_flag(
    args: &mut Args<'_>,
    display: &mut DisplayArgs,
    flag: &str,
    inline: Option<&str>,
) -> Result<bool, CliError> {
    match flag {
        "--color" => {
            let value = args.value(flag, inline)?;
            display.color = ColorChoice::from_name(&value)
                .ok_or_else(|| bad(flag, &value, "expected auto, truecolor, 256 or mono"))?;
        }
        "--no-bg" => display.no_bg = true,
        "--no-braille" => display.no_braille = true,
        "--no-anim" => display.no_anim = true,
        _ => return Ok(false),
    }
    Ok(true)
}

/// Applies a fleet flag to `fleet`. Returns whether `flag` was one.
fn fleet_flag(
    args: &mut Args<'_>,
    fleet: &mut FleetArgs,
    flag: &str,
    inline: Option<&str>,
) -> Result<bool, CliError> {
    match flag {
        "--name" => fleet.name = args.value(flag, inline)?,
        "--nodes" => {
            let value = args.value(flag, inline)?;
            fleet.nodes = parsed(flag, &value, str::parse::<usize>)?;
            if !(1..=6).contains(&fleet.nodes) {
                return Err(bad(flag, &value, "must be from 1 to 6"));
            }
        }
        "--base-ip" => {
            let value = args.value(flag, inline)?;
            fleet.base_ip = Some(parsed(flag, &value, str::parse::<Ipv4Addr>)?);
        }
        "--testnode" => fleet.testnode = Some(args.value(flag, inline)?.into()),
        "--owners" => {
            let value = args.value(flag, inline)?;
            let owners = parsed(flag, &value, str::parse::<u8>)?;
            fleet.owners = NonZeroU8::new(owners)
                .filter(|owners| owners.get() >= 2)
                .ok_or_else(|| bad(flag, &value, "must be at least 2"))?;
        }
        "--keys" => {
            let value = args.value(flag, inline)?;
            fleet.keys = parsed(flag, &value, str::parse::<u64>)?;
        }
        "--rate" => {
            let value = args.value(flag, inline)?;
            fleet.rate = parsed(flag, &value, str::parse::<u64>)?;
        }
        "--logs" => fleet.logs = Some(args.value(flag, inline)?.into()),
        _ => return Ok(false),
    }
    Ok(true)
}

/// The `watch` options as they accumulate, before the cross-flag checks.
struct WatchDraft {
    args: WatchArgs,
    once: bool,
    json: bool,
    settle: Option<Duration>,
    explain: Option<KeySpec>,
    cache: Option<String>,
}

/// Applies a `watch`-only flag to `draft`. Returns whether `flag` was one.
fn watch_flag(
    args: &mut Args<'_>,
    draft: &mut WatchDraft,
    flag: &str,
    inline: Option<&str>,
) -> Result<bool, CliError> {
    match flag {
        "--seed" => {
            let value = args.value(flag, inline)?;
            draft.args.seeds.push(parsed(flag, &value, Seed::parse)?);
        }
        "--bind" => {
            let value = args.value(flag, inline)?;
            draft.args.bind = parsed(flag, &value, str::parse::<SocketAddr>)?;
        }
        "--advertise" => {
            let value = args.value(flag, inline)?;
            draft.args.advertise = Some(parsed(flag, &value, str::parse::<IpAddr>)?);
        }
        "--metrics" => draft.args.metrics.push(args.value(flag, inline)?),
        "--scrape" => {
            let value = args.value(flag, inline)?;
            let (node, url) = value
                .split_once('=')
                .filter(|(node, url)| !node.is_empty() && !url.is_empty())
                .ok_or_else(|| bad(flag, &value, "expected NODE=URL"))?;
            draft.args.scrape.push(ScrapePin {
                node: node.to_owned(),
                url: url.to_owned(),
            });
        }
        "--interval" => {
            let value = args.value(flag, inline)?;
            draft.args.interval = positive_duration(flag, &value)?;
        }
        "--forget-after" => {
            let value = args.value(flag, inline)?;
            draft.args.forget_after = positive_duration(flag, &value)?;
        }
        "--exit-after" => {
            let value = args.value(flag, inline)?;
            draft.args.exit_after = Some(positive_duration(flag, &value)?);
        }
        "--log" => draft.args.log = Some(PathBuf::from(args.value(flag, inline)?)),
        "--once" => draft.once = true,
        "--json" => draft.json = true,
        "--settle" => {
            let value = args.value(flag, inline)?;
            draft.settle = Some(positive_duration(flag, &value)?);
        }
        "--explain" => {
            let value = args.value(flag, inline)?;
            draft.explain = Some(parsed(flag, &value, KeySpec::parse)?);
        }
        "--cache" => draft.cache = Some(args.value(flag, inline)?),
        _ => return Ok(false),
    }
    Ok(true)
}

fn parse_watch(rest: &[String]) -> Result<WatchArgs, CliError> {
    let mut draft = WatchDraft {
        args: WatchArgs {
            cluster: String::new(),
            seeds: Vec::new(),
            bind: SocketAddr::from(([0, 0, 0, 0], 0)),
            advertise: None,
            metrics: Vec::new(),
            scrape: Vec::new(),
            interval: Duration::from_secs(1),
            forget_after: Duration::from_secs(90),
            display: DisplayArgs::default(),
            exit_after: None,
            log: None,
            once: None,
        },
        once: false,
        json: false,
        settle: None,
        explain: None,
        cache: None,
    };
    let mut cluster: Option<String> = None;

    let mut args = Args::new(rest);
    while let Some((flag, inline)) = args.next() {
        let inline = inline.as_deref();
        if display_flag(&mut args, &mut draft.args.display, &flag, inline)?
            || watch_flag(&mut args, &mut draft, &flag, inline)?
        {
            continue;
        }
        if flag.starts_with('-') {
            return Err(CliError::UnknownFlag(flag));
        }
        if cluster.is_some() {
            return Err(CliError::UnexpectedArg(flag));
        }
        cluster = Some(flag);
    }
    if draft.cache.is_some() && draft.explain.is_none() {
        return Err(CliError::Requires {
            flag: "--cache",
            needs: "--explain",
        });
    }
    if !draft.once {
        if draft.explain.is_some() {
            return Err(CliError::Requires {
                flag: "--explain",
                needs: "--once",
            });
        }
        if draft.json {
            return Err(CliError::Requires {
                flag: "--json",
                needs: "--once",
            });
        }
        if draft.settle.is_some() {
            return Err(CliError::Requires {
                flag: "--settle",
                needs: "--once",
            });
        }
    }
    draft.args.cluster = cluster.ok_or(CliError::MissingCluster)?;
    draft.args.once = draft.once.then(|| OnceArgs {
        json: draft.json,
        settle: draft.settle.unwrap_or(Duration::from_secs(3)),
        explain: draft.explain.take().map(|key| ExplainArgs {
            key,
            cache: draft.cache.take(),
        }),
    });
    Ok(draft.args)
}

fn parse_cluster(rest: &[String]) -> Result<FleetArgs, CliError> {
    let mut fleet = FleetArgs::default();
    let mut args = Args::new(rest);
    while let Some((flag, inline)) = args.next() {
        if !fleet_flag(&mut args, &mut fleet, &flag, inline.as_deref())? {
            return Err(unknown_or_positional(flag));
        }
    }
    Ok(fleet)
}

fn parse_demo(rest: &[String]) -> Result<DemoArgs, CliError> {
    let mut demo = DemoArgs {
        scenario: ScenarioSource::Tour,
        headless: false,
        no_captions: false,
        marks: None,
        log: None,
        fleet: FleetArgs::default(),
        display: DisplayArgs::default(),
    };
    let mut args = Args::new(rest);
    while let Some((flag, inline)) = args.next() {
        let inline = inline.as_deref();
        if fleet_flag(&mut args, &mut demo.fleet, &flag, inline)?
            || display_flag(&mut args, &mut demo.display, &flag, inline)?
        {
            continue;
        }
        match flag.as_str() {
            "--scenario" => {
                let value = args.value(&flag, inline)?;
                demo.scenario = if value == "tour" {
                    ScenarioSource::Tour
                } else {
                    ScenarioSource::File(value.into())
                };
            }
            "--headless" => demo.headless = true,
            "--no-captions" => demo.no_captions = true,
            "--marks" => demo.marks = Some(args.value(&flag, inline)?.into()),
            "--log" => demo.log = Some(args.value(&flag, inline)?.into()),
            _ => return Err(unknown_or_positional(flag)),
        }
    }
    Ok(demo)
}

/// The error for an argument no flag table took.
fn unknown_or_positional(arg: String) -> CliError {
    if arg.starts_with('-') {
        CliError::UnknownFlag(arg)
    } else {
        CliError::UnexpectedArg(arg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn watch(args: &[&str]) -> WatchArgs {
        match parse(args.iter().copied()).unwrap() {
            Command::Watch(watch) => watch,
            other => panic!("not a watch: {other:?}"),
        }
    }

    fn err(args: &[&str]) -> CliError {
        parse(args.iter().copied()).unwrap_err()
    }

    #[test]
    fn no_arguments_and_help_flags_give_help() {
        assert_eq!(parse(Vec::<String>::new()), Ok(Command::Help));
        assert_eq!(parse(["--help"]), Ok(Command::Help));
        assert_eq!(parse(["-h"]), Ok(Command::Help));
        assert_eq!(parse(["watch", "x", "--help"]), Ok(Command::Help));
        assert_eq!(parse(["demo", "-h"]), Ok(Command::Help));
    }

    #[test]
    fn a_bare_cluster_name_means_watch_with_defaults() {
        let args = watch(&["lens-demo"]);
        assert_eq!(args.cluster, "lens-demo");
        assert!(args.seeds.is_empty() && args.metrics.is_empty() && args.scrape.is_empty());
        assert_eq!(args.bind, "0.0.0.0:0".parse().unwrap());
        assert_eq!(args.advertise, None);
        assert_eq!(args.interval, Duration::from_secs(1));
        assert_eq!(args.forget_after, Duration::from_secs(90));
        assert_eq!(args.display, DisplayArgs::default());
        assert_eq!(args.exit_after, None);
        assert_eq!(args.log, None);
        assert_eq!(args.once, None);
        assert_eq!(watch(&["watch", "lens-demo"]), args);
    }

    #[test]
    fn every_watch_flag_parses() {
        let args = watch(&[
            "watch",
            "prod",
            "--seed",
            "10.0.0.1:7946",
            "--seed",
            "node2.internal:7946",
            "--bind",
            "127.0.0.1:0",
            "--advertise",
            "10.9.9.9",
            "--metrics",
            "http://{ip}:9090/metrics",
            "--metrics",
            "http://{ip}:9091/metrics",
            "--scrape",
            "n1=http://h:1/metrics",
            "--scrape",
            "10.0.0.2:7946=http://h:2/metrics?x=1",
            "--interval",
            "500ms",
            "--forget-after",
            "2m",
            "--color",
            "256",
            "--no-bg",
            "--no-braille",
            "--no-anim",
            "--exit-after",
            "10s",
            "--log",
            "/tmp/lens.log",
        ]);
        assert_eq!(args.cluster, "prod");
        assert_eq!(
            args.seeds,
            [
                Seed::Addr("10.0.0.1:7946".parse().unwrap()),
                Seed::Host("node2.internal".into(), 7946)
            ]
        );
        assert_eq!(args.bind, "127.0.0.1:0".parse().unwrap());
        assert_eq!(args.advertise, Some("10.9.9.9".parse().unwrap()));
        assert_eq!(args.metrics.len(), 2);
        assert_eq!(
            args.scrape,
            [
                ScrapePin {
                    node: "n1".into(),
                    url: "http://h:1/metrics".into()
                },
                ScrapePin {
                    node: "10.0.0.2:7946".into(),
                    url: "http://h:2/metrics?x=1".into()
                }
            ]
        );
        assert_eq!(args.interval, Duration::from_millis(500));
        assert_eq!(args.forget_after, Duration::from_secs(120));
        assert_eq!(
            args.display,
            DisplayArgs {
                color: ColorChoice::Ansi256,
                no_bg: true,
                no_braille: true,
                no_anim: true
            }
        );
        assert_eq!(args.exit_after, Some(Duration::from_secs(10)));
        assert_eq!(args.log, Some(PathBuf::from("/tmp/lens.log")));
    }

    #[test]
    fn flags_take_an_equals_value() {
        let args = watch(&[
            "--seed=10.0.0.1:7946",
            "prod",
            "--interval=2s",
            "--scrape=n1=http://h/m",
        ]);
        assert_eq!(args.cluster, "prod");
        assert_eq!(args.seeds.len(), 1);
        assert_eq!(args.interval, Duration::from_secs(2));
        assert_eq!(args.scrape[0].url, "http://h/m");
    }

    #[test]
    fn the_cluster_name_may_follow_the_flags() {
        let args = watch(&["--seed", "10.0.0.1:7946", "prod"]);
        assert_eq!(args.cluster, "prod");
    }

    #[test]
    fn a_cluster_named_like_a_command_needs_the_watch_word() {
        let args = watch(&["watch", "demo"]);
        assert_eq!(args.cluster, "demo");
    }

    #[test]
    fn once_takes_json_and_settle() {
        let args = watch(&["prod", "--once"]);
        assert_eq!(
            args.once,
            Some(OnceArgs {
                json: false,
                settle: Duration::from_secs(3),
                explain: None,
            })
        );
        let args = watch(&["prod", "--once", "--json", "--settle", "500ms"]);
        assert_eq!(
            args.once,
            Some(OnceArgs {
                json: true,
                settle: Duration::from_millis(500),
                explain: None,
            })
        );
    }

    #[test]
    fn json_and_settle_need_once() {
        assert_eq!(
            err(&["prod", "--json"]),
            CliError::Requires {
                flag: "--json",
                needs: "--once"
            }
        );
        assert_eq!(
            err(&["prod", "--settle", "1s"]),
            CliError::Requires {
                flag: "--settle",
                needs: "--once"
            }
        );
    }

    #[test]
    fn explain_takes_a_key_and_a_cache() {
        let explain = |args: &[&str]| watch(args).once.and_then(|once| once.explain);
        let key = |text: &str| KeySpec::parse(text).expect("the key parses");

        assert_eq!(explain(&["prod", "--once"]), None);
        assert_eq!(
            explain(&["prod", "--once", "--explain", "k17"]),
            Some(ExplainArgs {
                key: key("k17"),
                cache: None,
            })
        );
        assert_eq!(
            explain(&["prod", "--once", "--explain", "uint:7", "--cache", "ids"]),
            Some(ExplainArgs {
                key: key("uint:7"),
                cache: Some("ids".to_owned()),
            })
        );
        // The flags take an equals value and come in either order, and the
        // key keeps every character after the first equals sign.
        assert_eq!(
            explain(&["--cache=it", "--once", "--explain=str:a=b", "prod"]),
            Some(ExplainArgs {
                key: key("str:a=b"),
                cache: Some("it".to_owned()),
            })
        );
        // The key is a value even when it looks like a flag.
        assert_eq!(
            explain(&["prod", "--once", "--explain", "--json"]),
            Some(ExplainArgs {
                key: key("--json"),
                cache: None,
            })
        );
        // It combines with the other `--once` options.
        let once = watch(&[
            "prod",
            "--once",
            "--json",
            "--settle",
            "1s",
            "--explain",
            "hex:6b",
        ])
        .once
        .expect("--once is given");
        assert!(once.json);
        assert_eq!(once.settle, Duration::from_secs(1));
        assert_eq!(once.explain.map(|e| e.key), Some(key("hex:6b")));
        assert_eq!(
            err(&["prod", "--once", "--explain"]),
            CliError::MissingValue("--explain".into())
        );
        assert_eq!(
            err(&["prod", "--once", "--explain", "k", "--cache"]),
            CliError::MissingValue("--cache".into())
        );
    }

    #[test]
    fn explain_needs_once_and_cache_needs_explain() {
        assert_eq!(
            err(&["prod", "--explain", "k1"]),
            CliError::Requires {
                flag: "--explain",
                needs: "--once"
            }
        );
        assert_eq!(
            err(&["prod", "--explain", "k1", "--cache", "it"]),
            CliError::Requires {
                flag: "--explain",
                needs: "--once"
            }
        );
        for args in [
            &["prod", "--cache", "it"][..],
            &["prod", "--once", "--cache", "it"],
        ] {
            assert_eq!(
                err(args),
                CliError::Requires {
                    flag: "--cache",
                    needs: "--explain"
                },
                "{args:?}"
            );
        }
        assert_eq!(
            CliError::Requires {
                flag: "--cache",
                needs: "--explain"
            }
            .to_string(),
            "--cache needs --explain"
        );
        // Neither flag belongs to the fleet commands.
        assert_eq!(
            err(&["cluster", "--explain", "k"]),
            CliError::UnknownFlag("--explain".into())
        );
        assert_eq!(
            err(&["demo", "--cache", "it"]),
            CliError::UnknownFlag("--cache".into())
        );
    }

    #[test]
    fn a_bad_key_is_a_bad_value_naming_explain() {
        for (value, remedy) in [
            ("hex:abc", "add or drop a digit"),
            ("hex:", "at least one pair"),
            ("hex:0g", "remove it"),
            ("uint:-1", "write a negative key as int:"),
            ("uint:x", "uint: takes decimal digits"),
            ("int:99999999999999999999", "use hex: for a wider key"),
        ] {
            let error = err(&["prod", "--once", "--explain", value]);
            let CliError::BadValue {
                flag,
                value: given,
                reason,
            } = &error
            else {
                panic!("{value}: {error:?}");
            };
            assert_eq!((flag.as_str(), given.as_str()), ("--explain", value));
            assert!(reason.contains(remedy), "{value}: {reason}");
            assert!(error.to_string().starts_with("--explain: bad value"));
        }
        let long = "k".repeat(crate::key::MAX_CHARS + 1);
        assert!(matches!(
            err(&["prod", "--once", "--explain", &long]),
            CliError::BadValue { reason, .. } if reason.contains("shorten it")
        ));
    }

    #[test]
    fn color_modes_parse_and_others_fail() {
        for (name, choice) in [
            ("auto", ColorChoice::Auto),
            ("truecolor", ColorChoice::Truecolor),
            ("256", ColorChoice::Ansi256),
            ("mono", ColorChoice::Mono),
        ] {
            assert_eq!(watch(&["x", "--color", name]).display.color, choice);
        }
        assert!(matches!(
            err(&["x", "--color", "rgb"]),
            CliError::BadValue { .. }
        ));
    }

    #[test]
    fn durations_take_ms_s_m_and_h() {
        assert_eq!(parse_duration("500ms"), Ok(Duration::from_millis(500)));
        assert_eq!(parse_duration("2s"), Ok(Duration::from_secs(2)));
        assert_eq!(parse_duration("1m"), Ok(Duration::from_secs(60)));
        assert_eq!(parse_duration("2h"), Ok(Duration::from_secs(7200)));
        assert_eq!(parse_duration("0s"), Ok(Duration::ZERO));
    }

    #[test]
    fn bad_durations_fail() {
        for text in [
            "",
            "5",
            "s",
            "1.5s",
            "-1s",
            "5x",
            "5 s",
            "99999999999999999999s",
        ] {
            assert!(parse_duration(text).is_err(), "{text:?}");
        }
        assert!(parse_duration(&format!("{}m", u64::MAX)).is_err());
        assert!(parse_duration(&format!("{}h", u64::MAX)).is_err());
    }

    #[test]
    fn a_zero_duration_is_refused_for_intervals() {
        for flag in ["--interval", "--forget-after", "--exit-after"] {
            assert!(
                matches!(err(&["x", flag, "0s"]), CliError::BadValue { .. }),
                "{flag}"
            );
        }
        assert!(matches!(
            err(&["x", "--once", "--settle", "0s"]),
            CliError::BadValue { .. }
        ));
    }

    #[test]
    fn seeds_parse_as_addresses_or_hosts() {
        assert_eq!(
            Seed::parse("127.0.0.11:7946"),
            Ok(Seed::Addr("127.0.0.11:7946".parse().unwrap()))
        );
        assert_eq!(
            Seed::parse("[::1]:7946"),
            Ok(Seed::Addr("[::1]:7946".parse().unwrap()))
        );
        assert_eq!(
            Seed::parse("node1:7946"),
            Ok(Seed::Host("node1".into(), 7946))
        );
        for text in [
            "node1",
            ":7946",
            "node1:",
            "node1:abc",
            "node1:99999",
            "a:b:7946",
        ] {
            assert!(Seed::parse(text).is_err(), "{text}");
        }
        assert_eq!(Seed::parse("node1:7946").unwrap().to_string(), "node1:7946");
        assert_eq!(
            Seed::parse("127.0.0.1:7946").unwrap().to_string(),
            "127.0.0.1:7946"
        );
    }

    #[test]
    fn bad_flag_values_name_the_flag() {
        for args in [
            &["x", "--bind", "nope"][..],
            &["x", "--advertise", "nope"],
            &["x", "--seed", "nope"],
            &["x", "--scrape", "n1"],
            &["x", "--scrape", "=http://h"],
            &["x", "--scrape", "n1="],
            &["x", "--interval", "fast"],
        ] {
            match err(args) {
                CliError::BadValue { flag, .. } => assert_eq!(flag, args[1]),
                other => panic!("{args:?}: {other:?}"),
            }
        }
    }

    #[test]
    fn missing_values_and_clusters_are_errors() {
        assert_eq!(
            err(&["x", "--seed"]),
            CliError::MissingValue("--seed".into())
        );
        assert_eq!(err(&["x", "--log"]), CliError::MissingValue("--log".into()));
        assert_eq!(err(&["watch"]), CliError::MissingCluster);
        assert_eq!(err(&["--no-bg"]), CliError::MissingCluster);
    }

    #[test]
    fn unknown_flags_and_extra_positionals_are_errors() {
        assert_eq!(
            err(&["x", "--bogus"]),
            CliError::UnknownFlag("--bogus".into())
        );
        assert_eq!(err(&["x", "-z"]), CliError::UnknownFlag("-z".into()));
        assert_eq!(err(&["x", "y"]), CliError::UnexpectedArg("y".into()));
        assert_eq!(
            err(&["cluster", "--once"]),
            CliError::UnknownFlag("--once".into())
        );
        assert_eq!(
            err(&["cluster", "stray"]),
            CliError::UnexpectedArg("stray".into())
        );
    }

    #[test]
    fn cluster_defaults_and_flags() {
        assert_eq!(
            parse(["cluster"]),
            Ok(Command::Cluster(FleetArgs::default()))
        );
        let fleet = FleetArgs::default();
        assert_eq!(fleet.name, "lens-demo");
        assert_eq!(fleet.nodes, 3);
        assert_eq!(fleet.base_ip, None);
        assert_eq!(fleet.owners.get(), 2);
        assert_eq!((fleet.keys, fleet.rate), (20_000, 1_500));
        assert_eq!(fleet.testnode, None);
        assert_eq!(fleet.logs, None);

        let Command::Cluster(fleet) = parse([
            "cluster",
            "--name",
            "other",
            "--nodes",
            "5",
            "--base-ip",
            "127.0.0.21",
            "--testnode",
            "/bin/tn",
            "--owners",
            "3",
            "--keys",
            "100",
            "--rate",
            "50",
            "--logs",
            "/tmp/l",
        ])
        .unwrap() else {
            panic!("not a cluster");
        };
        assert_eq!(fleet.name, "other");
        assert_eq!(fleet.nodes, 5);
        assert_eq!(fleet.base_ip, Some(Ipv4Addr::new(127, 0, 0, 21)));
        assert_eq!(fleet.testnode, Some(PathBuf::from("/bin/tn")));
        assert_eq!(fleet.owners.get(), 3);
        assert_eq!((fleet.keys, fleet.rate), (100, 50));
        assert_eq!(fleet.logs, Some(PathBuf::from("/tmp/l")));
    }

    #[test]
    fn fleet_limits_are_enforced() {
        for args in [
            &["cluster", "--nodes", "0"][..],
            &["cluster", "--nodes", "7"],
            &["cluster", "--nodes", "x"],
            &["cluster", "--owners", "1"],
            &["cluster", "--owners", "0"],
            &["cluster", "--owners", "256"],
            &["cluster", "--base-ip", "nope"],
            &["cluster", "--keys", "-1"],
            &["cluster", "--rate", "fast"],
        ] {
            assert!(
                matches!(err(args), CliError::BadValue { .. }),
                "{args:?}: {:?}",
                err(args)
            );
        }
    }

    #[test]
    fn demo_defaults_and_flags() {
        let Command::Demo(demo) = parse(["demo"]).unwrap() else {
            panic!("not a demo");
        };
        assert_eq!(demo.scenario, ScenarioSource::Tour);
        assert!(!demo.headless && !demo.no_captions);
        assert_eq!(demo.marks, None);
        assert_eq!(demo.log, None);
        assert_eq!(demo.fleet, FleetArgs::default());
        assert_eq!(demo.display, DisplayArgs::default());

        let Command::Demo(demo) = parse([
            "demo",
            "--scenario",
            "tour.txt",
            "--headless",
            "--no-captions",
            "--marks",
            "/tmp/marks",
            "--log",
            "/tmp/demo.log",
            "--nodes",
            "4",
            "--color",
            "mono",
            "--no-anim",
        ])
        .unwrap() else {
            panic!("not a demo");
        };
        assert_eq!(demo.scenario, ScenarioSource::File("tour.txt".into()));
        assert!(demo.headless && demo.no_captions);
        assert_eq!(demo.marks, Some(PathBuf::from("/tmp/marks")));
        assert_eq!(demo.log, Some(PathBuf::from("/tmp/demo.log")));
        assert_eq!(demo.fleet.nodes, 4);
        assert_eq!(demo.display.color, ColorChoice::Mono);
        assert!(demo.display.no_anim);
        assert_eq!(parse(["demo", "--scenario", "tour"]), parse(["demo"]));
    }

    #[test]
    fn demo_refuses_watch_only_flags() {
        assert_eq!(
            err(&["demo", "--seed", "x"]),
            CliError::UnknownFlag("--seed".into())
        );
        assert_eq!(
            err(&["demo", "stray"]),
            CliError::UnexpectedArg("stray".into())
        );
    }

    #[test]
    fn errors_display_a_reason() {
        assert!(
            CliError::UnknownFlag("--z".into())
                .to_string()
                .contains("--z")
        );
        assert!(
            CliError::MissingValue("--seed".into())
                .to_string()
                .contains("needs a value")
        );
        assert!(CliError::MissingCluster.to_string().contains("cluster"));
        assert!(
            CliError::UnexpectedArg("y".into())
                .to_string()
                .contains('y')
        );
        assert!(
            CliError::Requires {
                flag: "--json",
                needs: "--once"
            }
            .to_string()
            .contains("--once")
        );
        assert!(
            bad("--bind", "nope", "invalid")
                .to_string()
                .contains("--bind")
        );
    }

    #[test]
    fn the_help_text_names_every_command_and_flag() {
        for word in [
            "watch",
            "cluster",
            "demo",
            "--seed",
            "--bind",
            "--advertise",
            "--metrics",
            "--scrape",
            "--interval",
            "--forget-after",
            "--color",
            "--no-bg",
            "--no-braille",
            "--no-anim",
            "--exit-after",
            "--log",
            "--once",
            "--json",
            "--settle",
            "--explain",
            "--cache",
            "--name",
            "--nodes",
            "--base-ip",
            "--testnode",
            "--owners",
            "--keys",
            "--rate",
            "--logs",
            "--scenario",
            "--headless",
            "--no-captions",
            "--marks",
        ] {
            assert!(HELP.contains(word), "{word}");
        }
    }

    #[test]
    fn the_help_text_gives_the_key_grammar() {
        for word in ["uint:N", "int:N", "hex:BYTES", "str:TEXT", "String"] {
            assert!(HELP.contains(word), "{word}");
        }
        assert!(HELP.contains("[--explain KEY [--cache NAME]]"));
        assert!(
            HELP.contains(&format!(
                "waiting up to {} s",
                crate::once::Limits::default().extras.as_secs()
            )),
            "the help names the wait the run makes"
        );
        assert!(
            HELP.lines().all(|line| line.chars().count() <= 87),
            "no help line is wider than 87 columns"
        );
    }
}
