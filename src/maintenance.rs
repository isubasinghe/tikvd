use std::sync::Arc;

use tonic::codegen::BoxStream;
use tonic::{Request, Response, Status};

use crate::kv_service::{MEMBER_ID, RAFT_TERM, header, unsupported};
use crate::pb::etcdserverpb::maintenance_server::Maintenance;
use crate::pb::etcdserverpb::{
    AlarmRequest, AlarmResponse, DefragmentRequest, DefragmentResponse, DowngradeRequest,
    DowngradeResponse, HashKvRequest, HashKvResponse, HashRequest, HashResponse, MoveLeaderRequest,
    MoveLeaderResponse, SnapshotRequest, SnapshotResponse, StatusRequest, StatusResponse,
};
use crate::watch_hub::WatchHub;

const ETCD_VERSION: &str = "3.5.13";

pub struct MaintenanceService {
    hub: Arc<WatchHub>,
}

impl MaintenanceService {
    pub fn new(hub: Arc<WatchHub>) -> Self {
        MaintenanceService { hub }
    }
}

#[tonic::async_trait]
impl Maintenance for MaintenanceService {
    type SnapshotStream = BoxStream<SnapshotResponse>;

    async fn status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        Ok(Response::new(StatusResponse {
            header: Some(header(self.hub.watermark())),
            version: ETCD_VERSION.to_owned(),
            db_size: 0,
            leader: MEMBER_ID,
            raft_term: RAFT_TERM,
            ..Default::default()
        }))
    }

    async fn alarm(
        &self,
        _request: Request<AlarmRequest>,
    ) -> Result<Response<AlarmResponse>, Status> {
        Err(unsupported("alarm"))
    }

    async fn defragment(
        &self,
        _request: Request<DefragmentRequest>,
    ) -> Result<Response<DefragmentResponse>, Status> {
        Err(unsupported("defragment"))
    }

    async fn hash(&self, _request: Request<HashRequest>) -> Result<Response<HashResponse>, Status> {
        Err(unsupported("hash"))
    }

    async fn hash_kv(
        &self,
        _request: Request<HashKvRequest>,
    ) -> Result<Response<HashKvResponse>, Status> {
        Err(unsupported("hash_kv"))
    }

    async fn snapshot(
        &self,
        _request: Request<SnapshotRequest>,
    ) -> Result<Response<Self::SnapshotStream>, Status> {
        Err(unsupported("snapshot"))
    }

    async fn move_leader(
        &self,
        _request: Request<MoveLeaderRequest>,
    ) -> Result<Response<MoveLeaderResponse>, Status> {
        Err(unsupported("move_leader"))
    }

    async fn downgrade(
        &self,
        _request: Request<DowngradeRequest>,
    ) -> Result<Response<DowngradeResponse>, Status> {
        Err(unsupported("downgrade"))
    }
}
