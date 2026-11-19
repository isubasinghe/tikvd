#![allow(clippy::result_large_err)]

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, warn};

use crate::keys::parse_ttl_key;
use crate::kv_service::{header, unsupported};
use crate::pb::etcdserverpb::lease_server::Lease;
use crate::pb::etcdserverpb::{
    LeaseGrantRequest, LeaseGrantResponse, LeaseKeepAliveRequest, LeaseKeepAliveResponse,
    LeaseLeasesRequest, LeaseLeasesResponse, LeaseRevokeRequest, LeaseRevokeResponse,
    LeaseTimeToLiveRequest, LeaseTimeToLiveResponse,
};
use crate::store::{Store, StoreError};

pub const REAP_INTERVAL: Duration = Duration::from_secs(1);
const REAP_BATCH: u32 = 1000;

pub fn spawn_reaper(store: Store, interval: Duration) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if let Err(error) = reap_expired(&store).await {
                warn!(%error, "reaper tick failed");
            }
        }
    })
}

async fn reap_expired(store: &Store) -> Result<(), StoreError> {
    let index_keys = store.expired_ttl_keys(unix_now(), REAP_BATCH).await?;
    for index_key in index_keys {
        let Some((expiry, key)) = parse_ttl_key(&index_key) else {
            warn!(?index_key, "unparseable ttl index key");
            continue;
        };
        match store.delete_if_expired(key, expiry).await {
            Ok(true) => debug!(key = %String::from_utf8_lossy(key), "expired key deleted"),
            Ok(false) => {}
            Err(error) => warn!(%error, "could not reap an expired key"),
        }
    }
    Ok(())
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}

pub struct LeaseService {
    store: Store,
}

impl LeaseService {
    pub fn new(store: Store) -> Self {
        LeaseService { store }
    }
}

#[tonic::async_trait]
impl Lease for LeaseService {
    async fn lease_grant(
        &self,
        request: Request<LeaseGrantRequest>,
    ) -> Result<Response<LeaseGrantResponse>, Status> {
        let request = request.into_inner();
        debug!(rpc = "lease_grant", ttl = request.ttl, id = request.id);
        if request.ttl <= 0 {
            return Err(Status::invalid_argument(
                "etcdserver: lease ttl must be positive",
            ));
        }
        if request.id != 0 {
            return Err(Status::invalid_argument(
                "etcdserver: client-chosen lease ids are not supported",
            ));
        }
        let revision = self
            .store
            .now()
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        Ok(Response::new(LeaseGrantResponse {
            header: Some(header(revision)),
            id: request.ttl,
            ttl: request.ttl,
            error: String::new(),
        }))
    }

    async fn lease_revoke(
        &self,
        _request: Request<LeaseRevokeRequest>,
    ) -> Result<Response<LeaseRevokeResponse>, Status> {
        Err(unsupported("lease_revoke"))
    }

    type LeaseKeepAliveStream = ReceiverStream<Result<LeaseKeepAliveResponse, Status>>;

    async fn lease_keep_alive(
        &self,
        _request: Request<Streaming<LeaseKeepAliveRequest>>,
    ) -> Result<Response<Self::LeaseKeepAliveStream>, Status> {
        Err(unsupported("lease_keep_alive"))
    }

    async fn lease_time_to_live(
        &self,
        _request: Request<LeaseTimeToLiveRequest>,
    ) -> Result<Response<LeaseTimeToLiveResponse>, Status> {
        Err(unsupported("lease_time_to_live"))
    }

    async fn lease_leases(
        &self,
        _request: Request<LeaseLeasesRequest>,
    ) -> Result<Response<LeaseLeasesResponse>, Status> {
        Err(unsupported("lease_leases"))
    }
}
