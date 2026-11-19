use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;
use tracing::error;

use crate::cdc::feed::{FeedConfig, FeedHealth, FeedOutput, spawn_feed};
use crate::kv_service::KvService;
use crate::lease::{LeaseService, REAP_INTERVAL, spawn_reaper};
use crate::pb::etcdserverpb::kv_server::KvServer;
use crate::pb::etcdserverpb::lease_server::LeaseServer;
use crate::pb::etcdserverpb::watch_server::WatchServer;
use crate::store::Store;
use crate::watch_hub::WatchHub;
use crate::watch_service::WatchService;

const WATCH_WINDOW_EVENTS: usize = 100_000;
const WATCH_SUBSCRIBER_BUFFER: usize = 1024;

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
    if config.tls.is_some() {
        return Err("tls is not supported yet".into());
    }
    let store = Store::connect(config.pd_endpoints).await?;
    let (feed, feed_health) = spawn_feed(store.clone(), FeedConfig::default());
    let state = ServerState {
        store,
        hub: WatchHub::new(WATCH_WINDOW_EVENTS, WATCH_SUBSCRIBER_BUFFER),
        feed_health,
    };
    let mut pump = tokio::spawn(pump_feed(feed, state.hub.clone()));
    let reaper = spawn_reaper(state.store.clone(), REAP_INTERVAL);
    let incoming = TcpIncoming::from_listener(listener, true, None)?;
    let server = Server::builder()
        .add_service(LeaseServer::new(LeaseService::new(state.store.clone())))
        .add_service(KvServer::new(KvService::new(state.store.clone())))
        .add_service(WatchServer::new(WatchService::new(
            state.store.clone(),
            state.hub.clone(),
        )))
        .serve_with_incoming_shutdown(incoming, shutdown);
    let served = tokio::select! {
        served = server => served,
        _ = &mut pump => {
            error!("cdc feed closed; watches can no longer be served");
            reaper.abort();
            return Err("cdc feed closed".into());
        }
    };
    pump.abort();
    reaper.abort();
    served?;
    Ok(())
}

async fn pump_feed(mut feed: mpsc::Receiver<FeedOutput>, hub: Arc<WatchHub>) {
    while let Some(output) = feed.recv().await {
        hub.apply(output);
    }
}
