use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic_health::ServingStatus;
use tonic_health::server::{HealthReporter, health_reporter};
use tracing::error;

use crate::cdc::feed::{FeedConfig, FeedHealth, FeedOutput, spawn_feed};
use crate::kv_service::KvService;
use crate::lease::{LeaseService, REAP_INTERVAL, spawn_reaper};
use crate::maintenance::MaintenanceService;
use crate::pb::etcdserverpb::kv_server::KvServer;
use crate::pb::etcdserverpb::lease_server::LeaseServer;
use crate::pb::etcdserverpb::maintenance_server::MaintenanceServer;
use crate::pb::etcdserverpb::watch_server::WatchServer;
use crate::store::Store;
use crate::unimplemented_log::UnimplementedLogLayer;
use crate::watch_hub::WatchHub;
use crate::watch_service::WatchService;

const WATCH_WINDOW_EVENTS: usize = 100_000;
const WATCH_SUBSCRIBER_BUFFER: usize = 1024;
const HEALTH_INTERVAL: Duration = Duration::from_secs(1);
const HEALTH_SERVICES: [&str; 2] = ["", "etcdserverpb.Maintenance"];

type ServeError = Box<dyn std::error::Error + Send + Sync>;

pub struct Config {
    pub listen: SocketAddr,
    pub pd_endpoints: Vec<String>,
    pub tls: Option<(PathBuf, PathBuf)>,
}

pub struct ServerState {
    pub store: Store,
    pub hub: Arc<WatchHub>,
    pub feed_health: FeedHealth,
}

pub async fn serve(config: Config, shutdown: impl Future<Output = ()>) -> Result<(), ServeError> {
    let listener = TcpListener::bind(config.listen).await?;
    serve_listener(listener, config, shutdown).await
}

pub async fn serve_listener(
    listener: TcpListener,
    config: Config,
    shutdown: impl Future<Output = ()>,
) -> Result<(), ServeError> {
    let mut builder = match &config.tls {
        Some((cert, key)) => Server::builder().tls_config(server_tls(cert, key)?)?,
        None => Server::builder(),
    }
    .layer(UnimplementedLogLayer);
    let store = Store::connect(config.pd_endpoints).await?;
    let incoming = TcpIncoming::from_listener(listener, true, None)?;
    let (feed, feed_health) = spawn_feed(store.clone(), FeedConfig::default());
    let state = ServerState {
        store,
        hub: WatchHub::new(WATCH_WINDOW_EVENTS, WATCH_SUBSCRIBER_BUFFER),
        feed_health,
    };
    let (mut health_reporter, health_service) = health_reporter();
    publish_health(&mut health_reporter, &state.feed_health).await;
    let mut pump = tokio::spawn(pump_feed(feed, state.hub.clone()));
    let background = [
        spawn_reaper(state.store.clone(), REAP_INTERVAL),
        tokio::spawn(report_health(health_reporter, state.feed_health.clone())),
    ];
    let server = builder
        .add_service(health_service)
        .add_service(LeaseServer::new(LeaseService::new(state.store.clone())))
        .add_service(KvServer::new(KvService::new(state.store.clone())))
        .add_service(WatchServer::new(WatchService::new(
            state.store.clone(),
            state.hub.clone(),
        )))
        .add_service(MaintenanceServer::new(MaintenanceService::new(
            state.hub.clone(),
        )))
        .serve_with_incoming_shutdown(incoming, shutdown);
    let served = tokio::select! {
        served = server => served,
        _ = &mut pump => {
            error!("cdc feed closed; watches can no longer be served");
            background.iter().for_each(JoinHandle::abort);
            return Err("cdc feed closed".into());
        }
    };
    pump.abort();
    background.iter().for_each(JoinHandle::abort);
    served?;
    Ok(())
}

fn server_tls(cert: &Path, key: &Path) -> Result<ServerTlsConfig, ServeError> {
    let identity = Identity::from_pem(std::fs::read(cert)?, std::fs::read(key)?);
    Ok(ServerTlsConfig::new().identity(identity))
}

async fn report_health(mut reporter: HealthReporter, feed_health: FeedHealth) {
    let mut tick = tokio::time::interval(HEALTH_INTERVAL);
    loop {
        tick.tick().await;
        publish_health(&mut reporter, &feed_health).await;
    }
}

async fn publish_health(reporter: &mut HealthReporter, feed_health: &FeedHealth) {
    let status = if feed_health.is_serving() {
        ServingStatus::Serving
    } else {
        ServingStatus::NotServing
    };
    for service in HEALTH_SERVICES {
        reporter.set_service_status(service, status).await;
    }
}

async fn pump_feed(mut feed: mpsc::Receiver<FeedOutput>, hub: Arc<WatchHub>) {
    while let Some(output) = feed.recv().await {
        hub.apply(output);
    }
}
