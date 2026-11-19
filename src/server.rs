use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use crate::kv_service::KvService;
use crate::pb::etcdserverpb::kv_server::KvServer;
use crate::store::Store;

type ServeError = Box<dyn std::error::Error + Send + Sync>;

pub struct Config {
    pub listen: SocketAddr,
    pub pd_endpoints: Vec<String>,
    pub tls: Option<(PathBuf, PathBuf)>,
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
    Server::builder()
        .add_service(KvServer::new(KvService::new(store)))
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
        .await?;
    Ok(())
}
