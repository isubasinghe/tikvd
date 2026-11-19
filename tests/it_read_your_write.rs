mod common;

use std::collections::HashSet;
use std::future::Future;
use std::time::Duration;

use common::{start_tikvd, unique_prefix};
use etcd_client::{
    Client, Compare, CompareOp, PutOptions, Txn, TxnOp, TxnResponse, WatchOptions, WatchStream,
};
use tikvd::kv_service::KvService;
use tikvd::pb::etcdserverpb::kv_server::Kv;
use tikvd::pb::etcdserverpb::{
    self as pb, DeleteRangeRequest, PutRequest, RequestOp, TxnRequest, compare, request_op,
};
use tikvd::store::Store;
use tikvd::watch_hub::WatchHub;
use tokio::time::{Instant, timeout_at};
use tonic::Request;

const CLIENT_SCHEDULING: Duration = Duration::from_millis(5);
const EVENTUALLY: Duration = Duration::from_secs(5);
const KEYS: usize = 100;
const LEASE_TTL: i64 = 600;

struct WatchedRevisions {
    stream: WatchStream,
    seen: HashSet<i64>,
}

impl WatchedRevisions {
    async fn received_within(&mut self, revision: i64, allowance: Duration) -> bool {
        let deadline = Instant::now() + allowance;
        while !self.seen.contains(&revision) {
            let Ok(message) = timeout_at(deadline, self.stream.message()).await else {
                return false;
            };
            let response = message
                .expect("the watch stream is healthy")
                .expect("the watch stream is open");
            assert!(!response.canceled(), "watch cancelled: {response:?}");
            self.seen.extend(
                response
                    .events()
                    .iter()
                    .map(|event| event.kv().unwrap().mod_revision()),
            );
        }
        true
    }
}

struct Lateness {
    writes: usize,
    late: Vec<(String, Duration)>,
}

impl Lateness {
    async fn check(&mut self, watched: &mut WatchedRevisions, what: String, revision: i64) {
        self.writes += 1;
        if watched.received_within(revision, CLIENT_SCHEDULING).await {
            return;
        }
        let started = Instant::now();
        assert!(
            watched.received_within(revision, EVENTUALLY).await,
            "{what} at {revision} never arrived"
        );
        self.late
            .push((what, CLIENT_SCHEDULING + started.elapsed()));
    }
}

fn revision_of(response: &TxnResponse) -> i64 {
    response.header().expect("a header").revision()
}

async fn txn(client: &mut Client, key: &str, expected: i64, then: TxnOp) -> i64 {
    let response = client
        .txn(
            Txn::new()
                .when([Compare::mod_revision(key, CompareOp::Equal, expected)])
                .and_then([then])
                .or_else([TxnOp::get(key, None)]),
        )
        .await
        .unwrap();
    assert!(response.succeeded(), "txn on {key} at {expected} failed");
    revision_of(&response)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_is_in_the_same_instances_watch_stream_when_its_response_arrives() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let prefix = unique_prefix("it-ryw");
    let (_watcher, stream) = client
        .watch(prefix.as_str(), Some(WatchOptions::new().with_prefix()))
        .await
        .unwrap();
    let mut watched = WatchedRevisions {
        stream,
        seen: HashSet::new(),
    };
    let mut lateness = Lateness {
        writes: 0,
        late: Vec::new(),
    };
    let leased = || Some(PutOptions::new().with_lease(LEASE_TTL));

    for n in 0..KEYS {
        let key = format!("{prefix}{n:03}");
        let created = txn(&mut client, &key, 0, TxnOp::put(key.as_str(), "v1", None)).await;
        lateness
            .check(&mut watched, format!("create {key}"), created)
            .await;
        let updated = txn(
            &mut client,
            &key,
            created,
            TxnOp::put(key.as_str(), "v2", leased()),
        )
        .await;
        lateness
            .check(&mut watched, format!("leased update {key}"), updated)
            .await;
        let deleted = txn(
            &mut client,
            &key,
            updated,
            TxnOp::delete(key.as_str(), None),
        )
        .await;
        lateness
            .check(&mut watched, format!("delete {key}"), deleted)
            .await;
    }
    for n in 0..KEYS / 10 {
        let key = format!("{prefix}plain-{n:02}");
        let put = client.put(key.as_str(), "v", leased()).await.unwrap();
        let revision = put.header().unwrap().revision();
        lateness
            .check(&mut watched, format!("plain leased put {key}"), revision)
            .await;
        let deleted = client.delete(key.as_str(), None).await.unwrap();
        let revision = deleted.header().unwrap().revision();
        lateness
            .check(&mut watched, format!("plain delete {key}"), revision)
            .await;
    }

    assert!(lateness.writes >= 300, "only {} writes", lateness.writes);
    assert!(
        lateness.late.is_empty(),
        "{} of {} writes were not in the watch stream within {CLIENT_SCHEDULING:?} of their response; first: {:?}",
        lateness.late.len(),
        lateness.writes,
        &lateness.late[..lateness.late.len().min(10)]
    );
}

async fn service_with_a_stalled_feed() -> Option<KvService> {
    let Ok(pd) = std::env::var("TIKVD_TEST_PD") else {
        println!("skipping: TIKVD_TEST_PD is unset");
        return None;
    };
    let store = Store::connect(vec![pd]).await.unwrap();
    let hub_that_never_gets_a_watermark = WatchHub::new(16, 1 << 20, 16, 1 << 20);
    Some(KvService::new(store, hub_that_never_gets_a_watermark))
}

fn put_request(key: &str) -> PutRequest {
    PutRequest {
        key: key.as_bytes().to_vec(),
        value: b"v".to_vec(),
        ..Default::default()
    }
}

fn stale_update(key: &str) -> TxnRequest {
    TxnRequest {
        compare: vec![pb::Compare {
            result: compare::CompareResult::Equal as i32,
            target: compare::CompareTarget::Mod as i32,
            key: key.as_bytes().to_vec(),
            target_union: Some(compare::TargetUnion::ModRevision(1)),
            range_end: Vec::new(),
        }],
        success: vec![RequestOp {
            request: Some(request_op::Request::RequestPut(put_request(key))),
        }],
        failure: Vec::new(),
    }
}

async fn timed<T>(call: impl Future<Output = T>) -> (T, Duration) {
    let started = Instant::now();
    let result = call.await;
    (result, started.elapsed())
}

#[tokio::test]
async fn a_write_that_is_never_delivered_is_acknowledged_after_the_deadline() {
    let Some(service) = service_with_a_stalled_feed().await else {
        return;
    };
    let key = format!("{}a", unique_prefix("it-ryw-stalled"));

    let (put, took) = timed(service.put(Request::new(put_request(&key)))).await;

    assert!(put.unwrap().into_inner().header.unwrap().revision > 0);
    assert!(
        took >= Duration::from_millis(950) && took < Duration::from_secs(3),
        "took {took:?}"
    );
}

#[tokio::test]
async fn writes_that_change_nothing_do_not_wait_for_delivery() {
    let Some(service) = service_with_a_stalled_feed().await else {
        return;
    };
    let key = format!("{}missing", unique_prefix("it-ryw-stalled"));
    let delete = DeleteRangeRequest {
        key: key.as_bytes().to_vec(),
        ..Default::default()
    };

    let (rejected, rejected_took) = timed(service.txn(Request::new(stale_update(&key)))).await;
    let (deleted, deleted_took) = timed(service.delete_range(Request::new(delete))).await;

    assert!(!rejected.unwrap().into_inner().succeeded);
    assert_eq!(deleted.unwrap().into_inner().deleted, 0);
    for took in [rejected_took, deleted_took] {
        assert!(took < Duration::from_millis(500), "took {took:?}");
    }
}
