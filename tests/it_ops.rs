mod common;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use common::{start_tikvd, unique_prefix};
use etcd_client::{Certificate, Client, ConnectOptions, Error, GetOptions, TlsOptions};
use tikv_client::{Timestamp, TimestampExt, TransactionOptions};
use tikvd::keys::data_key;
use tikvd::record::Record;
use tikvd::server::{Config, serve_listener};
use tikvd::store::{Store, Write, WriteOutcome};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, MutexGuard};
use tonic::transport::Endpoint;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

const COMPACTED: &str = "required revision has been compacted";
const FUTURE_REVISION: &str = "required revision is a future revision";
const SERVING_DEADLINE: Duration = Duration::from_secs(60);

async fn compaction_turn() -> MutexGuard<'static, ()> {
    static COMPACTION: Mutex<()> = Mutex::const_new(());
    COMPACTION.lock().await
}

fn revision_of(header: Option<&etcd_client::ResponseHeader>) -> i64 {
    header.expect("response carries a header").revision()
}

fn message_of(error: &Error) -> String {
    match error {
        Error::GRpcStatus(status) => status.message().to_owned(),
        other => panic!("expected a gRPC status, got {other:?}"),
    }
}

async fn put(client: &mut Client, key: &str, value: &str) -> i64 {
    revision_of(client.put(key, value, None).await.unwrap().header())
}

async fn get_at(client: &mut Client, key: &str, revision: i64) -> Result<Vec<u8>, Error> {
    let response = client
        .get(key, Some(GetOptions::new().with_revision(revision)))
        .await?;
    Ok(response.kvs()[0].value().to_vec())
}

#[tokio::test]
async fn status_reports_version() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let deadline = tokio::time::Instant::now() + SERVING_DEADLINE;
    loop {
        let status = client.status().await.unwrap();
        assert_eq!(status.version(), "3.5.13");
        if revision_of(status.header()) > 0 {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "status never reported a positive revision"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn compact_then_old_read_is_compacted() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let _turn = compaction_turn().await;
    let key = format!("{}a", unique_prefix("it-ops"));
    let r1 = put(&mut client, &key, "v1").await;
    let r2 = put(&mut client, &key, "v2").await;

    client.compact(r2, None).await.unwrap();

    let error = get_at(&mut client, &key, r1).await.unwrap_err();
    assert!(message_of(&error).contains(COMPACTED), "{error:?}");
    assert_eq!(get_at(&mut client, &key, r2).await.unwrap(), b"v2");
    assert_eq!(get_at(&mut client, &key, 0).await.unwrap(), b"v2");
}

#[tokio::test]
async fn compact_at_or_below_the_current_compaction_is_rejected() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let _turn = compaction_turn().await;
    let key = format!("{}a", unique_prefix("it-ops"));
    let revision = put(&mut client, &key, "v").await;

    client.compact(revision, None).await.unwrap();

    for again in [revision, revision - 1] {
        let error = client.compact(again, None).await.unwrap_err();
        assert!(message_of(&error).contains(COMPACTED), "{error:?}");
    }
}

#[tokio::test]
async fn compact_future_revision_rejected() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let key = format!("{}a", unique_prefix("it-ops"));
    let revision = put(&mut client, &key, "v").await;

    let error = client
        .compact(revision + (1 << 40), None)
        .await
        .unwrap_err();
    assert!(message_of(&error).contains(FUTURE_REVISION), "{error:?}");
}

async fn test_store() -> Option<(Store, String)> {
    let Ok(pd) = std::env::var("TIKVD_TEST_PD") else {
        println!("skipping: TIKVD_TEST_PD is unset");
        return None;
    };
    Some((Store::connect(vec![pd.clone()]).await.unwrap(), pd))
}

async fn store_put(store: &Store, key: &[u8], value: &str) -> i64 {
    let write = Write::Put {
        value: value.as_bytes().to_vec(),
        ttl_secs: None,
    };
    match store.write(key, None, write).await.unwrap() {
        WriteOutcome::Applied { revision, .. } => revision,
        rejected => panic!("unconditional put was rejected: {rejected:?}"),
    }
}

async fn pd_gc_safe_point(pd: &str) -> u64 {
    let mut stream = TcpStream::connect(pd).await.unwrap();
    let request =
        format!("GET /pd/api/v1/gc/safepoint HTTP/1.1\r\nHost: {pd}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    let field = "\"gc_safe_point\":";
    let at = response.find(field).expect("pd reports a gc safe point") + field.len();
    response[at..]
        .trim_start()
        .split(|c: char| !c.is_ascii_digit())
        .next()
        .unwrap()
        .parse()
        .unwrap()
}

#[tokio::test]
async fn gc_safepoint_runs_one_compaction_behind() {
    let Some((store, pd)) = test_store().await else {
        return;
    };
    let _turn = compaction_turn().await;
    let key = unique_prefix("it-ops").into_bytes();

    let r1 = store_put(&store, &key, "v1").await;
    store.compact(r1).await.unwrap();
    assert!(pd_gc_safe_point(&pd).await < r1 as u64);

    let r2 = store_put(&store, &key, "v2").await;
    store.compact(r2).await.unwrap();
    assert_eq!(pd_gc_safe_point(&pd).await, r1 as u64);

    let r3 = store_put(&store, &key, "v3").await;
    store.compact(r3).await.unwrap();
    assert_eq!(pd_gc_safe_point(&pd).await, r2 as u64);
}

#[tokio::test]
async fn a_raw_read_between_the_last_two_compactions_still_sees_its_value() {
    let Some((store, _)) = test_store().await else {
        return;
    };
    let _turn = compaction_turn().await;
    let prefix = unique_prefix("it-ops");

    let r1 = store_put(&store, prefix.as_bytes(), "v1").await;
    store.compact(r1).await.unwrap();
    let r2 = store_put(&store, prefix.as_bytes(), "v2").await;
    store.compact(r2).await.unwrap();

    let options = TransactionOptions::new_optimistic().read_only();
    let mut snapshot = store
        .client()
        .snapshot(Timestamp::from_version(r2 as u64 - 1), options);
    let raw = snapshot.get(data_key(prefix.as_bytes())).await.unwrap();
    let raw = raw.expect("the value current between the compactions survives gc");
    assert_eq!(Record::decode(&raw).unwrap().value, b"v1");
}

async fn write_five(client: &mut Client, prefix: &str) {
    for name in ["a", "b", "c", "d", "e"] {
        put(client, &format!("{prefix}{name}"), name).await;
    }
}

async fn first_page(client: &mut Client, prefix: &str) -> (i64, Vec<u8>) {
    let page = client
        .get(prefix, Some(GetOptions::new().with_prefix().with_limit(2)))
        .await
        .unwrap();
    assert!(page.more());
    let last = page.kvs().last().unwrap().key().to_vec();
    (revision_of(page.header()), last)
}

async fn rest_after(
    client: &mut Client,
    prefix: &str,
    last: &[u8],
    revision: i64,
) -> Result<Vec<String>, Error> {
    let mut from = last.to_vec();
    from.push(0);
    let mut range_end = prefix.as_bytes().to_vec();
    *range_end.last_mut().unwrap() += 1;
    let rest = client
        .get(
            from,
            Some(
                GetOptions::new()
                    .with_range(range_end)
                    .with_revision(revision),
            ),
        )
        .await?;
    Ok(rest
        .kvs()
        .iter()
        .map(|kv| String::from_utf8(kv.value().to_vec()).unwrap())
        .collect())
}

#[tokio::test]
async fn paged_list_continues_after_a_compaction_below_its_revision() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let _turn = compaction_turn().await;
    let prefix = unique_prefix("it-ops");
    let before = put(&mut client, &format!("{prefix}0"), "before").await;
    write_five(&mut client, &prefix).await;
    let (revision, last) = first_page(&mut client, &prefix).await;

    client.compact(before, None).await.unwrap();

    let rest = rest_after(&mut client, &prefix, &last, revision)
        .await
        .unwrap();
    assert_eq!(rest, ["b", "c", "d", "e"]);
}

#[tokio::test]
async fn paged_list_fails_after_a_compaction_above_its_revision() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let _turn = compaction_turn().await;
    let prefix = unique_prefix("it-ops");
    write_five(&mut client, &prefix).await;
    let (revision, last) = first_page(&mut client, &prefix).await;
    let after = put(&mut client, &format!("{prefix}z"), "after").await;

    client.compact(after, None).await.unwrap();

    let error = rest_after(&mut client, &prefix, &last, revision)
        .await
        .unwrap_err();
    assert!(message_of(&error).contains(COMPACTED), "{error:?}");
}

#[tokio::test]
async fn health_serving_after_start() {
    let Some((_, addr)) = start_tikvd().await else {
        return;
    };
    let channel = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut health = HealthClient::new(channel);
    let deadline = tokio::time::Instant::now() + SERVING_DEADLINE;
    for service in ["", "etcdserverpb.Maintenance"] {
        loop {
            let request = HealthCheckRequest {
                service: service.to_owned(),
            };
            let status = health.check(request).await.unwrap().into_inner().status;
            if status == ServingStatus::Serving as i32 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{service:?} never became serving"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

fn write_temp(name: &str, contents: &str) -> PathBuf {
    let path = std::env::temp_dir().join(unique_prefix(name).trim_end_matches('/'));
    std::fs::write(&path, contents).unwrap();
    path
}

async fn start_tls_tikvd(pd: String, cert: PathBuf, key: PathBuf) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let config = Config {
        listen: addr,
        pd_endpoints: vec![pd],
        tls: Some((cert, key)),
    };
    tokio::spawn(serve_listener(listener, config, std::future::pending()));
    addr
}

#[tokio::test]
async fn tls_round_trip() {
    let Ok(pd) = std::env::var("TIKVD_TEST_PD") else {
        println!("skipping: TIKVD_TEST_PD is unset");
        return;
    };
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let cert_pem = certified.cert.pem();
    let cert = write_temp("it-ops-cert", &cert_pem);
    let key = write_temp("it-ops-key", &certified.signing_key.serialize_pem());
    let addr = start_tls_tikvd(pd, cert, key).await;

    let tls = TlsOptions::new()
        .ca_certificate(Certificate::from_pem(cert_pem))
        .domain_name("localhost");
    let options = ConnectOptions::new().with_tls(tls);
    let mut client = Client::connect([format!("https://{addr}")], Some(options))
        .await
        .unwrap();

    let key = format!("{}a", unique_prefix("it-ops"));
    put(&mut client, &key, "secret").await;
    assert_eq!(get_at(&mut client, &key, 0).await.unwrap(), b"secret");
}
