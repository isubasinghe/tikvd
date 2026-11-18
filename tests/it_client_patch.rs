use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use tikv_client::{Key, Snapshot, Timestamp, TimestampExt, TransactionClient, TransactionOptions};

async fn test_client() -> Option<TransactionClient> {
    let Ok(pd) = std::env::var("TIKVD_TEST_PD") else {
        println!("skipping: TIKVD_TEST_PD is unset");
        return None;
    };
    Some(TransactionClient::new(vec![pd]).await.unwrap())
}

fn unique_prefix() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("k/it-client-patch-{}-{nanos}-{n}/", std::process::id())
}

async fn put_and_commit(client: &TransactionClient, key: &str, value: &str) -> Timestamp {
    let mut txn = client.begin_optimistic().await.unwrap();
    txn.put(key.to_owned(), value.to_owned()).await.unwrap();
    txn.commit().await.unwrap().unwrap()
}

async fn latest_snapshot(client: &TransactionClient) -> Snapshot {
    let now = client.current_timestamp().await.unwrap();
    client.snapshot(now, TransactionOptions::new_optimistic().read_only())
}

#[tokio::test]
async fn commit_ts_round_trips() {
    let Some(client) = test_client().await else {
        return;
    };
    let key = format!("{}a", unique_prefix());
    let committed = put_and_commit(&client, &key, "value-a").await;

    let mut snapshot = latest_snapshot(&client).await;
    let (value, commit_ts) = snapshot
        .get_with_commit_ts(key)
        .await
        .unwrap()
        .expect("key was just written");

    assert_eq!(value, b"value-a".to_vec());
    assert_eq!(commit_ts.version(), committed.version());
}

#[tokio::test]
async fn batch_returns_each_keys_own_commit_ts() {
    let Some(client) = test_client().await else {
        return;
    };
    let prefix = unique_prefix();
    let first_key = format!("{prefix}1");
    let second_key = format!("{prefix}2");
    let first_commit = put_and_commit(&client, &first_key, "one").await;
    let second_commit = put_and_commit(&client, &second_key, "two").await;
    assert_ne!(first_commit.version(), second_commit.version());

    let mut snapshot = latest_snapshot(&client).await;
    let commits: HashMap<Key, u64> = snapshot
        .batch_get_with_commit_ts(vec![first_key.clone(), second_key.clone()])
        .await
        .unwrap()
        .into_iter()
        .map(|(pair, commit_ts)| (pair.into_key(), commit_ts.version()))
        .collect();

    assert_eq!(commits.len(), 2);
    assert_eq!(commits[&Key::from(first_key)], first_commit.version());
    assert_eq!(commits[&Key::from(second_key)], second_commit.version());
}

#[tokio::test]
async fn batch_resolves_secondary_locks_after_two_phase_commit() {
    let Some(client) = test_client().await else {
        return;
    };
    let prefix = unique_prefix();
    let primary_key = format!("{prefix}primary");
    let secondary_key = format!("{prefix}secondary");

    let mut txn = client
        .begin_with_options(TransactionOptions::new_optimistic())
        .await
        .unwrap();
    txn.put(primary_key.clone(), "p".to_owned()).await.unwrap();
    txn.put(secondary_key.clone(), "s".to_owned())
        .await
        .unwrap();
    let committed = txn.commit().await.unwrap().unwrap().version();

    let mut snapshot = latest_snapshot(&client).await;
    let commits: HashMap<Key, u64> = snapshot
        .batch_get_with_commit_ts(vec![primary_key.clone(), secondary_key.clone()])
        .await
        .unwrap()
        .into_iter()
        .map(|(pair, commit_ts)| (pair.into_key(), commit_ts.version()))
        .collect();

    assert_eq!(commits.len(), 2);
    assert_eq!(commits[&Key::from(primary_key)], committed);
    assert_eq!(commits[&Key::from(secondary_key)], committed);
}

#[tokio::test]
async fn missing_key_is_none() {
    let Some(client) = test_client().await else {
        return;
    };
    let missing = format!("{}missing", unique_prefix());

    let mut txn = client.begin_optimistic().await.unwrap();
    assert!(
        txn.get_with_commit_ts(missing.clone())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        txn.batch_get_with_commit_ts(vec![missing])
            .await
            .unwrap()
            .is_empty()
    );
    txn.rollback().await.unwrap();
}

#[tokio::test]
async fn regions_cover_range() {
    let Some(client) = test_client().await else {
        return;
    };
    let start = b"k/".to_vec();
    let end = b"k0".to_vec();

    let routes = client
        .regions_in_range(start.clone()..end.clone())
        .await
        .unwrap();

    assert!(!routes.is_empty());
    let first = &routes[0].region;
    let last = &routes[routes.len() - 1].region;
    assert!(first.start_key <= start);
    assert!(last.end_key.is_empty() || last.end_key >= end);
    for pair in routes.windows(2) {
        assert!(pair[0].region.start_key < pair[1].region.start_key);
        assert_eq!(pair[0].region.end_key, pair[1].region.start_key);
    }
    for route in &routes {
        assert!(!route.store_address.is_empty());
        assert!(route.region.peers.contains(&route.leader));
    }
}
