//! `sundog-demo`: a plain CLI for exercising a `sundog` cluster — join,
//! put/get, watch events, print membership. Deliberately not a TUI (that's
//! M7 polish, per `docs/HOUSE_RULES.md`). Currently a stub: `Cluster::builder`
//! is still `todo!()`, so this only wires up logging and argument parsing.

use anyhow::Result;
use sundog::Cluster;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cluster_name = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "sundog-demo".to_owned());
    println!("sundog-demo starting: cluster_name={cluster_name}");

    let _builder = Cluster::builder(cluster_name);
    // TODO(M1): _builder.build().await?, then a put/get/watch/membership loop.

    Ok(())
}
