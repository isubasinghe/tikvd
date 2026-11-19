use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use tikvd::server::{Config, serve};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "0.0.0.0:2479")]
    listen: SocketAddr,
    #[arg(long = "pd", required = true)]
    pd_endpoints: Vec<String>,
    #[arg(long, requires = "key")]
    cert: Option<PathBuf>,
    #[arg(long, requires = "cert")]
    key: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let args = Args::parse();
    let config = Config {
        listen: args.listen,
        pd_endpoints: args.pd_endpoints,
        tls: args.cert.zip(args.key),
    };
    serve(config, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await
}
