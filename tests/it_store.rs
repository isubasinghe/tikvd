use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use tikv_client::TransactionOptions;
use tikvd::keys::{parse_ttl_key, ttl_range_until};
use tikvd::store::{Compare, KeyValue, RangeQuery, Store, StoreError, Write, WriteOutcome};

async fn test_store() -> Option<Store> {
    let Ok(pd) = std::env::var("TIKVD_TEST_PD") else {
        println!("skipping: TIKVD_TEST_PD is unset");
        return None;
    };
    Some(Store::connect(vec![pd]).await.unwrap())
}

fn unique_prefix() -> Vec<u8> {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("it-store-{}-{nanos}-{n}/", std::process::id()).into_bytes()
}

fn key(prefix: &[u8], suffix: &str) -> Vec<u8> {
    [prefix, suffix.as_bytes()].concat()
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn put(value: &str) -> Write {
    Write::Put {
        value: value.as_bytes().to_vec(),
        ttl_secs: None,
    }
}

fn single(key: &[u8]) -> RangeQuery {
    RangeQuery {
        key: key.to_vec(),
        range_end: Vec::new(),
        limit: 0,
        revision: 0,
        keys_only: false,
        count_only: false,
    }
}

fn prefix_query(prefix: &[u8]) -> RangeQuery {
    let mut range_end = prefix.to_vec();
    *range_end.last_mut().unwrap() += 1;
    RangeQuery {
        range_end,
        ..single(prefix)
    }
}

async fn applied(store: &Store, key: &[u8], compare: Option<Compare>, write: Write) -> i64 {
    match store.write(key, compare, write).await.unwrap() {
        WriteOutcome::Applied { revision, .. } => revision,
        rejected => panic!("expected applied, got {rejected:?}"),
    }
}

async fn get(store: &Store, key: &[u8]) -> Option<KeyValue> {
    let mut result = store.range(single(key)).await.unwrap();
    assert!(result.kvs.len() <= 1);
    result.kvs.pop()
}

#[tokio::test]
async fn create_then_get() {
    let Some(store) = test_store().await else {
        return;
    };
    let k = key(&unique_prefix(), "a");

    let outcome = store
        .write(&k, Some(Compare::ModRevision(0)), put("v1"))
        .await
        .unwrap();
    let WriteOutcome::Applied { revision, prev } = outcome else {
        panic!("expected applied, got {outcome:?}");
    };
    assert!(prev.is_none());

    let result = store.range(single(&k)).await.unwrap();
    assert_eq!(result.kvs.len(), 1);
    let kv = &result.kvs[0];
    assert_eq!(kv.key, k);
    assert_eq!(kv.value, b"v1");
    assert_eq!(kv.mod_revision, revision);
    assert_eq!(kv.create_revision, revision);
    assert_eq!(kv.version, 1);
    assert_eq!(kv.lease, 0);
    assert!(result.revision >= revision);
}

#[tokio::test]
async fn update_bumps_version_and_keeps_create_below_mod() {
    let Some(store) = test_store().await else {
        return;
    };
    let k = key(&unique_prefix(), "a");
    let first = applied(&store, &k, None, put("v1")).await;

    let outcome = store.write(&k, None, put("v2")).await.unwrap();
    let WriteOutcome::Applied { revision, prev } = outcome else {
        panic!("expected applied, got {outcome:?}");
    };
    assert_eq!(prev.unwrap().mod_revision, first);

    let kv = get(&store, &k).await.unwrap();
    assert_eq!(kv.value, b"v2");
    assert_eq!(kv.version, 2);
    assert_eq!(kv.mod_revision, revision);
    assert!(kv.create_revision < kv.mod_revision);
}

#[tokio::test]
async fn stale_compare_is_rejected_with_current() {
    let Some(store) = test_store().await else {
        return;
    };
    let k = key(&unique_prefix(), "a");
    let r = applied(&store, &k, None, put("v1")).await;

    let outcome = store
        .write(&k, Some(Compare::ModRevision(r - 1)), put("v2"))
        .await
        .unwrap();
    let WriteOutcome::Rejected {
        current: Some(kv), ..
    } = outcome
    else {
        panic!("expected rejected with current, got {outcome:?}");
    };
    assert_eq!(kv.mod_revision, r);
    assert_eq!(kv.value, b"v1");
    assert_eq!(get(&store, &k).await.unwrap().value, b"v1");
}

#[tokio::test]
async fn version_compare() {
    let Some(store) = test_store().await else {
        return;
    };
    let k = key(&unique_prefix(), "a");
    applied(&store, &k, None, put("v1")).await;

    applied(&store, &k, Some(Compare::Version(1)), put("v2")).await;
    let outcome = store
        .write(&k, Some(Compare::Version(5)), put("v3"))
        .await
        .unwrap();
    assert!(
        matches!(outcome, WriteOutcome::Rejected { .. }),
        "{outcome:?}"
    );
    assert_eq!(get(&store, &k).await.unwrap().value, b"v2");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_writes_get_distinct_revisions() {
    let Some(store) = test_store().await else {
        return;
    };
    let prefix = unique_prefix();

    let tasks: Vec<_> = (0..50)
        .map(|i| {
            let store = store.clone();
            let k = key(&prefix, &format!("distinct-{i}"));
            tokio::spawn(async move {
                store
                    .write(&k, Some(Compare::ModRevision(0)), put("v"))
                    .await
                    .unwrap()
            })
        })
        .collect();
    let mut revisions = Vec::new();
    for task in tasks {
        match task.await.unwrap() {
            WriteOutcome::Applied { revision, .. } => revisions.push(revision),
            rejected => panic!("expected applied, got {rejected:?}"),
        }
    }

    revisions.sort();
    revisions.dedup();
    assert_eq!(revisions.len(), 50, "revisions shared between writes");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_creates_one_winner() {
    let Some(store) = test_store().await else {
        return;
    };
    let k = key(&unique_prefix(), "contended");

    let tasks: Vec<_> = (0..20)
        .map(|i| {
            let store = store.clone();
            let k = k.clone();
            tokio::spawn(async move {
                let value = format!("writer-{i}");
                let outcome = store
                    .write(&k, Some(Compare::ModRevision(0)), put(&value))
                    .await
                    .unwrap();
                (value, outcome)
            })
        })
        .collect();
    let mut winners = Vec::new();
    let mut losers = Vec::new();
    for task in tasks {
        match task.await.unwrap() {
            (value, WriteOutcome::Applied { revision, .. }) => winners.push((value, revision)),
            (_, WriteOutcome::Rejected { current, .. }) => losers.push(current),
        }
    }

    assert_eq!(winners.len(), 1);
    assert_eq!(losers.len(), 19);
    let (winning_value, winning_revision) = &winners[0];
    for current in losers {
        let current = current.expect("loser sees the winner's key");
        assert_eq!(current.value, winning_value.as_bytes());
        assert_eq!(current.mod_revision, *winning_revision);
    }
}

#[tokio::test]
async fn read_at_old_revision() {
    let Some(store) = test_store().await else {
        return;
    };
    let k = key(&unique_prefix(), "a");
    let r1 = applied(&store, &k, None, put("v1")).await;
    let r2 = applied(&store, &k, None, put("v2")).await;
    assert!(r2 > r1);

    let result = store
        .range(RangeQuery {
            revision: r1,
            ..single(&k)
        })
        .await
        .unwrap();
    assert_eq!(result.revision, r1);
    assert_eq!(result.kvs.len(), 1);
    assert_eq!(result.kvs[0].value, b"v1");
    assert_eq!(result.kvs[0].mod_revision, r1);
}

#[tokio::test]
async fn paging() {
    let Some(store) = test_store().await else {
        return;
    };
    let prefix = unique_prefix();
    for name in ["a", "b", "c", "d", "e"] {
        applied(&store, &key(&prefix, name), None, put(name)).await;
    }

    let first = store
        .range(RangeQuery {
            limit: 2,
            ..prefix_query(&prefix)
        })
        .await
        .unwrap();
    assert_eq!(first.kvs.len(), 2);
    assert!(first.more);
    assert_eq!(first.count, 5);
    assert_eq!(first.kvs[0].key, key(&prefix, "a"));
    assert_eq!(first.kvs[1].key, key(&prefix, "b"));

    applied(&store, &key(&prefix, "bb"), None, put("late")).await;

    let next_key = [first.kvs[1].key.as_slice(), b"\0"].concat();
    let rest = store
        .range(RangeQuery {
            key: next_key,
            revision: first.revision,
            ..prefix_query(&prefix)
        })
        .await
        .unwrap();
    let names: Vec<_> = rest.kvs.iter().map(|kv| kv.value.clone()).collect();
    assert_eq!(names, vec![b"c".to_vec(), b"d".to_vec(), b"e".to_vec()]);
    assert!(!rest.more);
    assert_eq!(rest.count, 3);
    assert_eq!(rest.revision, first.revision);
}

#[tokio::test]
async fn edges() {
    let Some(store) = test_store().await else {
        return;
    };
    let prefix = [b"\xff\xfe".as_slice(), &unique_prefix()].concat();
    let ff_prefix = [prefix.as_slice(), b"p\xff"].concat();
    let child = [ff_prefix.as_slice(), b"/child"].concat();
    let empty = key(&prefix, "empty");
    applied(&store, &key(&prefix, "a"), None, put("a")).await;
    applied(&store, &child, None, put("child")).await;
    applied(&store, &empty, None, put("")).await;

    let from_key = store
        .range(RangeQuery {
            range_end: b"\0".to_vec(),
            ..single(&key(&prefix, "a"))
        })
        .await
        .unwrap();
    let keys: Vec<_> = from_key.kvs.iter().map(|kv| kv.key.clone()).collect();
    assert!(keys.contains(&key(&prefix, "a")));
    assert!(keys.contains(&child));
    assert!(keys.contains(&empty));
    assert!(
        keys.iter()
            .all(|k| k.as_slice() >= key(&prefix, "a").as_slice())
    );
    assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));

    let children = store
        .range(RangeQuery {
            range_end: key(&prefix, "q"),
            ..single(&ff_prefix)
        })
        .await
        .unwrap();
    assert_eq!(children.kvs.len(), 1);
    assert_eq!(children.kvs[0].key, child);

    let empty_kv = get(&store, &empty).await.unwrap();
    assert!(empty_kv.value.is_empty());
    assert_eq!(empty_kv.version, 1);
}

#[tokio::test]
async fn keys_only() {
    let Some(store) = test_store().await else {
        return;
    };
    let prefix = unique_prefix();
    let r = applied(&store, &key(&prefix, "a"), None, put("value")).await;

    let result = store
        .range(RangeQuery {
            keys_only: true,
            ..prefix_query(&prefix)
        })
        .await
        .unwrap();
    assert_eq!(result.kvs.len(), 1);
    assert_eq!(result.kvs[0].key, key(&prefix, "a"));
    assert!(result.kvs[0].value.is_empty());
    assert_eq!(result.kvs[0].mod_revision, r);
}

#[tokio::test]
async fn count_only() {
    let Some(store) = test_store().await else {
        return;
    };
    let prefix = unique_prefix();
    for name in ["a", "b", "c"] {
        applied(&store, &key(&prefix, name), None, put(name)).await;
    }

    let result = store
        .range(RangeQuery {
            count_only: true,
            ..prefix_query(&prefix)
        })
        .await
        .unwrap();
    assert!(result.kvs.is_empty());
    assert_eq!(result.count, 3);
}

#[tokio::test]
async fn delete_then_recreate_resets_version() {
    let Some(store) = test_store().await else {
        return;
    };
    let k = key(&unique_prefix(), "a");
    applied(&store, &k, None, put("v1")).await;
    applied(&store, &k, None, put("v2")).await;

    let outcome = store.write(&k, None, Write::Delete).await.unwrap();
    let WriteOutcome::Applied {
        prev: Some(prev), ..
    } = outcome
    else {
        panic!("expected applied with prev, got {outcome:?}");
    };
    assert_eq!(prev.value, b"v2");
    assert!(get(&store, &k).await.is_none());

    let recreated = applied(&store, &k, Some(Compare::ModRevision(0)), put("v3")).await;
    let kv = get(&store, &k).await.unwrap();
    assert_eq!(kv.version, 1);
    assert_eq!(kv.create_revision, kv.mod_revision);
    assert_eq!(kv.mod_revision, recreated);
}

#[tokio::test]
async fn delete_of_missing_key_applies_without_prev() {
    let Some(store) = test_store().await else {
        return;
    };
    let k = key(&unique_prefix(), "missing");
    let before = store.now().await.unwrap();

    let outcome = store.write(&k, None, Write::Delete).await.unwrap();
    let WriteOutcome::Applied { revision, prev } = outcome else {
        panic!("expected applied, got {outcome:?}");
    };
    assert!(prev.is_none());
    assert!(revision > before);
}

#[tokio::test]
async fn delete_range_removes_every_key_in_range() {
    let Some(store) = test_store().await else {
        return;
    };
    let prefix = unique_prefix();
    for name in ["a", "b", "c"] {
        applied(&store, &key(&prefix, name), None, put(name)).await;
    }

    let mut range_end = prefix.clone();
    *range_end.last_mut().unwrap() += 1;
    let (deleted, revision) = store.delete_range(&prefix, &range_end).await.unwrap();
    assert_eq!(deleted, 3);
    assert!(
        store
            .range(prefix_query(&prefix))
            .await
            .unwrap()
            .kvs
            .is_empty()
    );
    assert!(store.now().await.unwrap() > revision);
}

async fn ttl_index_contains(store: &Store, etcd_key: &[u8], until: u64) -> bool {
    let now = store.client().current_timestamp().await.unwrap();
    let mut snapshot = store
        .client()
        .snapshot(now, TransactionOptions::new_optimistic());
    let (start, end) = ttl_range_until(until);
    snapshot
        .scan_keys(start..end, u32::MAX)
        .await
        .unwrap()
        .any(|ttl_key| parse_ttl_key(ttl_key.as_ref()).map(|(_, k)| k) == Some(etcd_key))
}

#[tokio::test]
async fn put_with_ttl_writes_index() {
    let Some(store) = test_store().await else {
        return;
    };
    let k = key(&unique_prefix(), "leased");

    applied(
        &store,
        &k,
        None,
        Write::Put {
            value: b"v1".to_vec(),
            ttl_secs: Some(60),
        },
    )
    .await;
    let kv = get(&store, &k).await.unwrap();
    assert_ne!(kv.lease, 0);
    assert!(ttl_index_contains(&store, &k, unix_now() + 61).await);

    applied(&store, &k, None, put("v2")).await;
    assert_eq!(get(&store, &k).await.unwrap().lease, 0);
    assert!(!ttl_index_contains(&store, &k, unix_now() + 61).await);
}

#[tokio::test]
async fn now_is_a_fresh_timestamp() {
    let Some(store) = test_store().await else {
        return;
    };
    let first = store.now().await.unwrap();
    let second = store.now().await.unwrap();
    assert!(first > 0);
    assert!(second > first);
}

#[tokio::test]
async fn compact_rejects_non_positive_revision() {
    let Some(store) = test_store().await else {
        return;
    };
    assert!(matches!(store.compact(0).await, Err(StoreError::Compacted)));
    assert!(matches!(
        store.compact(-1).await,
        Err(StoreError::Compacted)
    ));
}

#[tokio::test]
async fn compact_rejects_future_revision() {
    let Some(store) = test_store().await else {
        return;
    };
    let future = store.now().await.unwrap() + (1 << 40);
    assert!(matches!(
        store.compact(future).await,
        Err(StoreError::FutureRevision)
    ));
}
