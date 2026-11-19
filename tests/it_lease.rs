mod common;

use std::time::Duration;

use common::{start_tikvd, unique_prefix};
use etcd_client::{
    Client, Compare, CompareOp, EventType, LeaseGrantOptions, PutOptions, Txn, TxnOp, WatchStream,
};
use tokio::time::{sleep, timeout};

const DEADLINE: Duration = Duration::from_secs(10);

async fn put_with_ttl(client: &mut Client, key: &str, ttl: i64) {
    let lease = client.lease_grant(ttl, None).await.unwrap();
    assert_eq!(lease.id(), ttl);
    assert_eq!(lease.ttl(), ttl);
    client
        .put(key, "v", Some(PutOptions::new().with_lease(lease.id())))
        .await
        .unwrap();
}

async fn key_exists(client: &mut Client, key: &str) -> bool {
    !client.get(key, None).await.unwrap().kvs().is_empty()
}

async fn delete_events_within(stream: &mut WatchStream, window: Duration) -> usize {
    let mut deletes = 0;
    let _ = timeout(window, async {
        while let Some(response) = stream.message().await.unwrap() {
            deletes += response
                .events()
                .iter()
                .filter(|event| event.event_type() == EventType::Delete)
                .count();
        }
    })
    .await;
    deletes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn key_with_ttl_disappears_and_watchers_see_delete() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let key = format!("{}a", unique_prefix("it-lease"));
    put_with_ttl(&mut client, &key, 1).await;
    let (_watcher, mut stream) = client.watch(key.as_str(), None).await.unwrap();

    let event = timeout(DEADLINE, async {
        loop {
            let response = stream.message().await.unwrap().unwrap();
            if let Some(event) = response.events().first() {
                return event.event_type();
            }
        }
    })
    .await
    .expect("a delete event within the deadline");

    assert_eq!(event, EventType::Delete);
    assert!(!key_exists(&mut client, &key).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rewrite_without_lease_survives() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let key = format!("{}a", unique_prefix("it-lease"));
    put_with_ttl(&mut client, &key, 1).await;
    client.put(key.as_str(), "v2", None).await.unwrap();

    sleep(Duration::from_secs(3)).await;

    assert!(key_exists(&mut client, &key).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_reapers_do_not_double_delete() {
    let Some((mut first, _)) = start_tikvd().await else {
        return;
    };
    let (mut second, _) = start_tikvd().await.unwrap();
    let key = format!("{}a", unique_prefix("it-lease"));
    let (_watcher, mut stream) = first.watch(key.as_str(), None).await.unwrap();
    put_with_ttl(&mut second, &key, 1).await;

    let deletes = delete_events_within(&mut stream, Duration::from_secs(6)).await;

    assert_eq!(deletes, 1);
    assert!(!key_exists(&mut first, &key).await);
}

#[tokio::test]
async fn kv_reports_lease() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let prefix = unique_prefix("it-lease");
    let leased = format!("{prefix}leased");
    let plain = format!("{prefix}plain");
    put_with_ttl(&mut client, &leased, 3600).await;
    client.put(plain.as_str(), "v", None).await.unwrap();

    let leased_kv = client.get(leased.as_str(), None).await.unwrap();
    let plain_kv = client.get(plain.as_str(), None).await.unwrap();

    assert_ne!(leased_kv.kvs()[0].lease(), 0);
    assert_eq!(plain_kv.kvs()[0].lease(), 0);
}

#[tokio::test]
async fn lease_grant_with_non_positive_ttl_is_invalid() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let error = client.lease_grant(0, None).await.unwrap_err();
    assert!(
        format!("{error:?}").contains("InvalidArgument"),
        "{error:?}"
    );
}

#[tokio::test]
async fn lease_grant_with_client_chosen_id_is_invalid() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let options = LeaseGrantOptions::new().with_id(9);
    let error = client.lease_grant(5, Some(options)).await.unwrap_err();
    assert!(
        format!("{error:?}").contains("InvalidArgument"),
        "{error:?}"
    );
}

#[tokio::test]
async fn put_with_negative_lease_is_invalid() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let key = format!("{}a", unique_prefix("it-lease"));
    let error = client
        .put(key.as_str(), "v", Some(PutOptions::new().with_lease(-1)))
        .await
        .unwrap_err();
    assert!(
        format!("{error:?}").contains("InvalidArgument"),
        "{error:?}"
    );
}

#[tokio::test]
async fn txn_put_with_lease_is_accepted() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let key = format!("{}a", unique_prefix("it-lease"));
    let txn = Txn::new()
        .when([Compare::mod_revision(key.as_str(), CompareOp::Equal, 0)])
        .and_then([TxnOp::put(
            key.as_str(),
            "v",
            Some(PutOptions::new().with_lease(3600)),
        )]);

    assert!(client.txn(txn).await.unwrap().succeeded());

    let kv = client.get(key.as_str(), None).await.unwrap();
    assert_ne!(kv.kvs()[0].lease(), 0);
}
