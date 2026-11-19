use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use tikvd::server::{Config, serve};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

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
    let stopping = stop_on_signal()?;
    let serving = serve(config, stopped(stopping.clone()));
    tokio::pin!(serving);
    tokio::select! {
        served = &mut serving => return served,
        _ = stopped(stopping) => {}
    }
    info!(grace = ?SHUTDOWN_GRACE, "shutting down");
    tokio::time::timeout(SHUTDOWN_GRACE, serving)
        .await
        .unwrap_or_else(|_| {
            warn!("connections still open after the shutdown grace period, exiting");
            Ok(())
        })
}

fn stop_on_signal() -> std::io::Result<watch::Receiver<bool>> {
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    let (stop, stopping) = watch::channel(false);
    tokio::spawn(async move {
        tokio::select! {
            _ = terminate.recv() => {}
            _ = interrupt.recv() => {}
        }
        stop.send_replace(true);
    });
    Ok(stopping)
}

async fn stopped(mut stopping: watch::Receiver<bool>) {
    let _ = stopping.wait_for(|stop| *stop).await;
}
