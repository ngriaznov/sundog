//! `sundog-demo`: a plain CLI for exercising a live `sundog` cluster — join
//! (mDNS by default, or a fixed seed list), open a replicated `String ->
//! String` cache, put/get/del from stdin, and print cluster events as they
//! arrive.

use std::net::SocketAddr;

use anyhow::{Context as _, bail};
use sundog::{Cluster, Event, Mode, Origin};
use tokio::io::{AsyncBufReadExt as _, BufReader};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let (cluster_name, seeds) = parse_args(std::env::args().skip(1))?;

    let mut builder = Cluster::builder(cluster_name.clone());
    if !seeds.is_empty() {
        builder = builder.seeds(seeds);
    }
    let cluster = builder.build().await.context("failed to form cluster")?;
    println!(
        "joined cluster {cluster_name:?} as node {}",
        cluster.node_id()
    );

    let cache = cluster
        .cache::<String, String>("demo")
        .mode(Mode::Replicated)
        .open()
        .await
        .context("failed to open the demo cache")?;

    let mut events = cache.events();
    tokio::spawn(async move {
        while let Ok(event) = events.recv().await {
            print_event(&event);
        }
    });

    println!("commands: put <k> <v> | get <k> | del <k> | peers | quit");
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = lines.next_line().await.context("reading stdin")? {
        match line.split_whitespace().collect::<Vec<_>>().as_slice() {
            ["put", key, value] => match cache.insert((*key).into(), (*value).into()).await {
                Ok(()) => println!("ok"),
                Err(error) => println!("error: {error}"),
            },
            ["get", key] => match cache.get(&(*key).to_string()).await {
                Some(value) => println!("{value}"),
                None => println!("(missing)"),
            },
            ["del", key] => match cache.remove(&(*key).to_string()).await {
                Ok(()) => println!("ok"),
                Err(error) => println!("error: {error}"),
            },
            ["peers"] => print_peers(&cluster),
            ["quit"] => break,
            [] => {}
            _ => println!("commands: put <k> <v> | get <k> | del <k> | peers | quit"),
        }
    }

    cluster.shutdown().await;
    Ok(())
}

fn print_peers(cluster: &Cluster) {
    let peers = cluster.peers();
    if peers.is_empty() {
        println!("(no other live peers)");
        return;
    }
    for peer in peers {
        println!("{}  node={}  data={}", peer.name, peer.node, peer.data_addr);
    }
}

fn print_event(event: &Event<String, String>) {
    let origin = match event {
        Event::Created { origin, .. }
        | Event::Updated { origin, .. }
        | Event::Removed { origin, .. } => describe_origin(*origin),
    };
    match event {
        Event::Created { key, value, .. } => {
            println!("[event] created {key:?}={value:?} ({origin})");
        }
        Event::Updated { key, value, .. } => {
            println!("[event] updated {key:?}={value:?} ({origin})");
        }
        Event::Removed { key, .. } => println!("[event] removed {key:?} ({origin})"),
    }
}

fn describe_origin(origin: Origin) -> String {
    match origin {
        Origin::Local => "local".to_owned(),
        Origin::Remote(node) => format!("remote:{node}"),
    }
}

/// Parses `--cluster <name>` and `--seeds <a,b,...>`; mDNS discovery is used
/// when `--seeds` is absent.
fn parse_args(mut args: impl Iterator<Item = String>) -> anyhow::Result<(String, Vec<SocketAddr>)> {
    let mut cluster_name = "sundog-demo".to_owned();
    let mut seeds = Vec::new();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--cluster" => cluster_name = args.next().context("--cluster needs a value")?,
            "--seeds" => {
                let raw = args.next().context("--seeds needs a value")?;
                for part in raw.split(',') {
                    seeds.push(
                        part.trim()
                            .parse()
                            .with_context(|| format!("invalid seed address {part:?}"))?,
                    );
                }
            }
            other => bail!("unrecognized argument: {other}"),
        }
    }
    Ok((cluster_name, seeds))
}
