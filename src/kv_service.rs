#![allow(clippy::result_large_err)]

use tonic::{Request, Response, Status};
use tracing::{debug, warn};

use crate::pb::etcdserverpb::kv_server::Kv;
use crate::pb::etcdserverpb::{
    CompactionRequest, CompactionResponse, DeleteRangeRequest, DeleteRangeResponse, PutRequest,
    PutResponse, RangeRequest, RangeResponse, RequestOp, ResponseHeader, ResponseOp, TxnRequest,
    TxnResponse, compare, request_op, response_op,
};
use crate::pb::mvccpb;
use crate::store::{self, RangeQuery, Store, StoreError, Write, WriteOutcome};

const CLUSTER_ID: u64 = 0x7469_6b76_6401;
pub(crate) const MEMBER_ID: u64 = 0x7469_6b76_6402;
pub(crate) const RAFT_TERM: u64 = 1;

const COMPACTED: &str = "etcdserver: mvcc: required revision has been compacted";
const FUTURE_REVISION: &str = "etcdserver: mvcc: required revision is a future revision";

pub struct KvService {
    store: Store,
}

impl KvService {
    pub fn new(store: Store) -> Self {
        KvService { store }
    }

    async fn range_response(&self, request: &RangeRequest) -> Result<RangeResponse, Status> {
        require_key(&request.key)?;
        if request.revision < 0 {
            return Err(Status::invalid_argument("etcdserver: invalid revision"));
        }
        if request.revision > 0 && request.revision > self.store.now().await.map_err(status)? {
            return Err(Status::out_of_range(FUTURE_REVISION));
        }
        reject_unsupported_range_options(request)?;

        let result = self
            .store
            .range(RangeQuery {
                key: request.key.clone(),
                range_end: request.range_end.clone(),
                limit: request.limit,
                revision: request.revision,
                keys_only: request.keys_only,
                count_only: request.count_only,
            })
            .await
            .map_err(status)?;
        Ok(RangeResponse {
            header: Some(header(result.revision)),
            kvs: result.kvs.into_iter().map(to_proto).collect(),
            more: result.more,
            count: result.count,
        })
    }

    async fn put_response(&self, request: &PutRequest) -> Result<PutResponse, Status> {
        require_key(&request.key)?;
        reject_unsupported_put_options(request)?;
        let write = Write::Put {
            value: request.value.clone(),
            ttl_secs: ttl_of(request.lease)?,
        };
        let outcome = self
            .store
            .write(&request.key, None, write)
            .await
            .map_err(status)?;
        let WriteOutcome::Applied { revision, prev } = outcome else {
            return Err(Status::internal("unconditional put was rejected"));
        };
        Ok(PutResponse {
            header: Some(header(revision)),
            prev_kv: prev.filter(|_| request.prev_kv).map(to_proto),
        })
    }

    async fn delete_range_response(
        &self,
        request: &DeleteRangeRequest,
    ) -> Result<DeleteRangeResponse, Status> {
        require_key(&request.key)?;
        if request.range_end.is_empty() {
            return self.delete_key(&request.key, request.prev_kv).await;
        }
        if request.prev_kv {
            return Err(unsupported("delete_range over a range with prev_kv"));
        }
        let (deleted, revision) = self
            .store
            .delete_range(&request.key, &request.range_end)
            .await
            .map_err(status)?;
        Ok(DeleteRangeResponse {
            header: Some(header(revision)),
            deleted,
            prev_kvs: Vec::new(),
        })
    }

    async fn delete_key(&self, key: &[u8], prev_kv: bool) -> Result<DeleteRangeResponse, Status> {
        let outcome = self
            .store
            .write(key, None, Write::Delete)
            .await
            .map_err(status)?;
        let WriteOutcome::Applied { revision, prev } = outcome else {
            return Err(Status::internal("unconditional delete was rejected"));
        };
        Ok(delete_response(revision, prev, prev_kv))
    }

    async fn txn_response(&self, request: &TxnRequest) -> Result<TxnResponse, Status> {
        let txn = parse_txn(request)?;
        let write = match &txn.write {
            TxnWrite::Put {
                value, ttl_secs, ..
            } => Write::Put {
                value: value.clone(),
                ttl_secs: *ttl_secs,
            },
            TxnWrite::Delete { .. } => Write::Delete,
        };
        let outcome = self
            .store
            .write(&txn.key, Some(txn.compare), write)
            .await
            .map_err(status)?;

        match outcome {
            WriteOutcome::Applied { revision, prev } => Ok(TxnResponse {
                header: Some(header(revision)),
                succeeded: true,
                responses: vec![applied_op(&txn.write, revision, prev)],
            }),
            WriteOutcome::Rejected { revision, current } => Ok(TxnResponse {
                header: Some(header(revision)),
                succeeded: false,
                responses: rejected_ops(txn.read_on_failure, revision, current),
            }),
        }
    }
}

#[tonic::async_trait]
impl Kv for KvService {
    async fn range(
        &self,
        request: Request<RangeRequest>,
    ) -> Result<Response<RangeResponse>, Status> {
        let request = request.into_inner();
        debug!(
            rpc = "range",
            key = %show(&request.key),
            range_end = %show(&request.range_end),
            limit = request.limit,
            revision = request.revision,
            keys_only = request.keys_only,
            count_only = request.count_only,
        );
        Ok(Response::new(self.range_response(&request).await?))
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let request = request.into_inner();
        debug!(
            rpc = "put",
            key = %show(&request.key),
            value_len = request.value.len(),
            lease = request.lease,
            prev_kv = request.prev_kv,
        );
        Ok(Response::new(self.put_response(&request).await?))
    }

    async fn delete_range(
        &self,
        request: Request<DeleteRangeRequest>,
    ) -> Result<Response<DeleteRangeResponse>, Status> {
        let request = request.into_inner();
        debug!(
            rpc = "delete_range",
            key = %show(&request.key),
            range_end = %show(&request.range_end),
            prev_kv = request.prev_kv,
        );
        Ok(Response::new(self.delete_range_response(&request).await?))
    }

    async fn txn(&self, request: Request<TxnRequest>) -> Result<Response<TxnResponse>, Status> {
        let request = request.into_inner();
        debug!(
            rpc = "txn",
            compares = request.compare.len(),
            success_ops = request.success.len(),
            failure_ops = request.failure.len(),
        );
        Ok(Response::new(self.txn_response(&request).await?))
    }

    async fn compact(
        &self,
        request: Request<CompactionRequest>,
    ) -> Result<Response<CompactionResponse>, Status> {
        let request = request.into_inner();
        debug!(rpc = "compact", revision = request.revision);
        let revision = self.store.compact(request.revision).await.map_err(status)?;
        Ok(Response::new(CompactionResponse {
            header: Some(header(revision)),
        }))
    }
}

enum TxnWrite {
    Put {
        value: Vec<u8>,
        ttl_secs: Option<i64>,
        prev_kv: bool,
    },
    Delete {
        prev_kv: bool,
    },
}

struct ParsedTxn {
    key: Vec<u8>,
    compare: store::Compare,
    write: TxnWrite,
    read_on_failure: bool,
}

fn parse_txn(request: &TxnRequest) -> Result<ParsedTxn, Status> {
    let [compare] = request.compare.as_slice() else {
        return Err(unsupported("txn without exactly one compare"));
    };
    let [success] = request.success.as_slice() else {
        return Err(unsupported("txn without exactly one success op"));
    };
    require_key(&compare.key)?;
    if !compare.range_end.is_empty() {
        return Err(unsupported("txn compare over a key range"));
    }
    if compare.result != compare::CompareResult::Equal as i32 {
        return Err(unsupported("txn compare other than equality"));
    }

    let write = match success.request.as_ref() {
        Some(request_op::Request::RequestPut(put)) => {
            reject_unsupported_put_options(put)?;
            if put.key != compare.key {
                return Err(unsupported("txn put on a key other than the compared key"));
            }
            TxnWrite::Put {
                value: put.value.clone(),
                ttl_secs: ttl_of(put.lease)?,
                prev_kv: put.prev_kv,
            }
        }
        Some(request_op::Request::RequestDeleteRange(delete)) => {
            if delete.key != compare.key || !delete.range_end.is_empty() {
                return Err(unsupported("txn delete other than on the compared key"));
            }
            TxnWrite::Delete {
                prev_kv: delete.prev_kv,
            }
        }
        _ => return Err(unsupported("txn success op other than put or delete_range")),
    };

    Ok(ParsedTxn {
        key: compare.key.clone(),
        compare: parse_compare(compare)?,
        write,
        read_on_failure: parse_failure(&request.failure, &compare.key)?,
    })
}

fn parse_compare(compare: &crate::pb::etcdserverpb::Compare) -> Result<store::Compare, Status> {
    match (compare.target_union.as_ref(), compare.target) {
        (Some(compare::TargetUnion::ModRevision(revision)), target)
            if target == compare::CompareTarget::Mod as i32 =>
        {
            Ok(store::Compare::ModRevision(*revision))
        }
        (Some(compare::TargetUnion::Version(version)), target)
            if target == compare::CompareTarget::Version as i32 =>
        {
            Ok(store::Compare::Version(*version))
        }
        _ => Err(unsupported(
            "txn compare target other than mod_revision or version",
        )),
    }
}

fn parse_failure(failure: &[RequestOp], compared_key: &[u8]) -> Result<bool, Status> {
    match failure {
        [] => Ok(false),
        [
            RequestOp {
                request: Some(request_op::Request::RequestRange(range)),
            },
        ] if range.key == compared_key && range.range_end.is_empty() => {
            reject_unsupported_else_range(range)?;
            Ok(true)
        }
        _ => Err(unsupported(
            "txn failure branch other than one range on the compared key",
        )),
    }
}

fn reject_unsupported_else_range(range: &RangeRequest) -> Result<(), Status> {
    reject_unsupported_range_options(range)?;
    if range.revision != 0 || range.count_only || range.keys_only || range.limit > 1 {
        return Err(unsupported(
            "txn failure range with revision, count_only, keys_only or a limit above 1",
        ));
    }
    Ok(())
}

fn applied_op(write: &TxnWrite, revision: i64, prev: Option<store::KeyValue>) -> ResponseOp {
    let response = match write {
        TxnWrite::Put { prev_kv, .. } => response_op::Response::ResponsePut(PutResponse {
            header: Some(header(revision)),
            prev_kv: prev.filter(|_| *prev_kv).map(to_proto),
        }),
        TxnWrite::Delete { prev_kv } => {
            response_op::Response::ResponseDeleteRange(delete_response(revision, prev, *prev_kv))
        }
    };
    ResponseOp {
        response: Some(response),
    }
}

fn rejected_ops(
    read_on_failure: bool,
    revision: i64,
    current: Option<store::KeyValue>,
) -> Vec<ResponseOp> {
    if !read_on_failure {
        return Vec::new();
    }
    let kvs: Vec<_> = current.into_iter().map(to_proto).collect();
    vec![ResponseOp {
        response: Some(response_op::Response::ResponseRange(RangeResponse {
            header: Some(header(revision)),
            count: kvs.len() as i64,
            kvs,
            more: false,
        })),
    }]
}

fn delete_response(
    revision: i64,
    prev: Option<store::KeyValue>,
    prev_kv: bool,
) -> DeleteRangeResponse {
    DeleteRangeResponse {
        header: Some(header(revision)),
        deleted: i64::from(prev.is_some()),
        prev_kvs: prev.filter(|_| prev_kv).map(to_proto).into_iter().collect(),
    }
}

fn reject_unsupported_range_options(request: &RangeRequest) -> Result<(), Status> {
    let sorted = request.sort_order != 0 || request.sort_target != 0;
    let filtered = request.min_mod_revision != 0
        || request.max_mod_revision != 0
        || request.min_create_revision != 0
        || request.max_create_revision != 0;
    if sorted || filtered {
        return Err(unsupported("range with sort or revision filters"));
    }
    Ok(())
}

fn reject_unsupported_put_options(request: &PutRequest) -> Result<(), Status> {
    if request.ignore_value || request.ignore_lease {
        return Err(unsupported("put with ignore_value or ignore_lease"));
    }
    Ok(())
}

fn ttl_of(lease: i64) -> Result<Option<i64>, Status> {
    match lease {
        0 => Ok(None),
        ttl if ttl > 0 => Ok(Some(ttl)),
        _ => Err(Status::invalid_argument(
            "etcdserver: lease id must not be negative",
        )),
    }
}

fn require_key(key: &[u8]) -> Result<(), Status> {
    if key.is_empty() {
        return Err(Status::invalid_argument("etcdserver: key is not provided"));
    }
    Ok(())
}

pub(crate) fn unsupported(shape: &str) -> Status {
    warn!(shape, "unsupported request");
    Status::unimplemented(format!("unsupported: {shape}"))
}

fn status(error: StoreError) -> Status {
    match error {
        StoreError::Compacted => Status::out_of_range(COMPACTED),
        StoreError::FutureRevision => Status::out_of_range(FUTURE_REVISION),
        StoreError::Corrupt(_) => Status::data_loss(error.to_string()),
        StoreError::Tikv(_) => Status::unavailable(error.to_string()),
    }
}

pub(crate) fn header(revision: i64) -> ResponseHeader {
    ResponseHeader {
        cluster_id: CLUSTER_ID,
        member_id: MEMBER_ID,
        revision,
        raft_term: RAFT_TERM,
    }
}

fn to_proto(kv: store::KeyValue) -> mvccpb::KeyValue {
    mvccpb::KeyValue {
        key: kv.key,
        create_revision: kv.create_revision,
        mod_revision: kv.mod_revision,
        version: kv.version,
        value: kv.value,
        lease: kv.lease,
    }
}

fn show(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::etcdserverpb::Compare;

    const KEY: &[u8] = b"k";

    fn txn_with_else(range: RangeRequest) -> TxnRequest {
        TxnRequest {
            compare: vec![Compare {
                result: compare::CompareResult::Equal as i32,
                target: compare::CompareTarget::Mod as i32,
                key: KEY.to_vec(),
                target_union: Some(compare::TargetUnion::ModRevision(0)),
                range_end: Vec::new(),
            }],
            success: vec![RequestOp {
                request: Some(request_op::Request::RequestPut(PutRequest {
                    key: KEY.to_vec(),
                    value: b"v".to_vec(),
                    ..Default::default()
                })),
            }],
            failure: vec![RequestOp {
                request: Some(request_op::Request::RequestRange(range)),
            }],
        }
    }

    fn plain_range() -> RangeRequest {
        RangeRequest {
            key: KEY.to_vec(),
            ..Default::default()
        }
    }

    fn assert_unimplemented(range: RangeRequest) {
        let error = parse_txn(&txn_with_else(range)).err().expect("rejected");
        assert_eq!(error.code(), tonic::Code::Unimplemented);
    }

    #[test]
    fn else_range_with_revision_is_unimplemented() {
        assert_unimplemented(RangeRequest {
            revision: 5,
            ..plain_range()
        });
    }

    #[test]
    fn else_range_with_count_only_is_unimplemented() {
        assert_unimplemented(RangeRequest {
            count_only: true,
            ..plain_range()
        });
    }

    #[test]
    fn else_range_with_keys_only_is_unimplemented() {
        assert_unimplemented(RangeRequest {
            keys_only: true,
            ..plain_range()
        });
    }

    #[test]
    fn else_range_with_sort_is_unimplemented() {
        assert_unimplemented(RangeRequest {
            sort_order: 1,
            ..plain_range()
        });
    }

    #[test]
    fn else_range_with_large_limit_is_unimplemented() {
        assert_unimplemented(RangeRequest {
            limit: 2,
            ..plain_range()
        });
    }

    fn txn_with_put(put: PutRequest) -> TxnRequest {
        let mut txn = txn_with_else(plain_range());
        txn.success[0].request = Some(request_op::Request::RequestPut(PutRequest {
            key: KEY.to_vec(),
            ..put
        }));
        txn
    }

    #[test]
    fn txn_put_with_a_lease_carries_the_ttl() {
        let txn = parse_txn(&txn_with_put(PutRequest {
            lease: 60,
            ..Default::default()
        }))
        .expect("accepted");
        let TxnWrite::Put { ttl_secs, .. } = txn.write else {
            panic!("expected a put");
        };
        assert_eq!(ttl_secs, Some(60));
    }

    #[test]
    fn txn_put_with_ignore_lease_is_unimplemented() {
        let error = parse_txn(&txn_with_put(PutRequest {
            ignore_lease: true,
            ..Default::default()
        }))
        .err()
        .expect("rejected");
        assert_eq!(error.code(), tonic::Code::Unimplemented);
    }

    #[test]
    fn txn_put_with_a_negative_lease_is_invalid() {
        let error = parse_txn(&txn_with_put(PutRequest {
            lease: -1,
            ..Default::default()
        }))
        .err()
        .expect("rejected");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn plain_else_range_on_the_compared_key_is_accepted() {
        for limit in [0, 1] {
            let range = RangeRequest {
                limit,
                ..plain_range()
            };
            let txn = parse_txn(&txn_with_else(range)).expect("accepted");
            assert!(txn.read_on_failure);
        }
    }
}
