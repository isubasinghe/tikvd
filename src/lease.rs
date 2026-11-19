#![allow(clippy::result_large_err)]

use std::time::Duration;

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
use crate::store::{MAX_TTL_SECS, Store, StoreError};

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
    let index_keys = store.expired_ttl_keys(REAP_BATCH).await?;
    // ponytail: a corrupt or unparseable ttl entry stays at the index head and is retried every tick, so REAP_BATCH of them stall reaping; delete such entries here if any appear
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

fn granted_ttl(request: &LeaseGrantRequest) -> Result<i64, Status> {
    if request.ttl <= 0 {
        return Err(Status::invalid_argument(
            "etcdserver: lease ttl must be positive",
        ));
    }
    if request.ttl > MAX_TTL_SECS {
        return Err(Status::invalid_argument("etcdserver: too large lease TTL"));
    }
    if request.id != 0 {
        return Err(Status::invalid_argument(
            "etcdserver: client-chosen lease ids are not supported",
        ));
    }
    Ok(request.ttl)
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
        let ttl = granted_ttl(&request)?;
        let revision = self
            .store
            .now()
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        Ok(Response::new(LeaseGrantResponse {
            header: Some(header(revision)),
            id: ttl,
            ttl,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn grant(ttl: i64) -> LeaseGrantRequest {
        LeaseGrantRequest { ttl, id: 0 }
    }

    #[test]
    fn lease_grant_ttl_larger_than_the_representable_range_is_rejected() {
        assert_eq!(granted_ttl(&grant(MAX_TTL_SECS)).unwrap(), MAX_TTL_SECS);
        for ttl in [MAX_TTL_SECS + 1, i64::MAX] {
            assert_eq!(
                granted_ttl(&grant(ttl)).unwrap_err().code(),
                tonic::Code::InvalidArgument
            );
        }
    }
}
