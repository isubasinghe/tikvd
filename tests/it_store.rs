use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use tikv_client::TransactionOptions;
use tikvd::keys::{data_key, parse_ttl_key, ttl_range_until};
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

async fn pd_unix_secs(store: &Store) -> u64 {
    let now = store.client().current_timestamp().await.unwrap();
    now.physical as u64 / 1000
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
async fn range_larger_than_a_default_grpc_message() {
    let Some(store) = test_store().await else {
        return;
    };
    let prefix = unique_prefix();
    let value = vec![b'x'; 1024 * 1024];
    let names = ["a", "b", "c", "d", "e", "f"];
    for name in names {
        let write = Write::Put {
            value: value.clone(),
            ttl_secs: None,
        };
        applied(&store, &key(&prefix, name), None, write).await;
    }

    let result = store
        .range(RangeQuery {
            limit: 10_000,
            ..prefix_query(&prefix)
        })
        .await
        .unwrap();
    assert_eq!(result.kvs.len(), names.len());
    assert!(result.kvs.iter().all(|kv| kv.value == value));
}

async fn write_mebibyte_values(store: &Store, prefix: &[u8], count: usize) -> Vec<(Vec<u8>, i64)> {
    let mut written = Vec::new();
    for n in 0..count {
        let name = key(prefix, &format!("{n:03}"));
        let write = Write::Put {
            value: vec![n as u8; 1024 * 1024],
            ttl_secs: None,
        };
        let revision = applied(store, &name, None, write).await;
        written.push((name, revision));
    }
    written
}

#[tokio::test]
async fn range_larger_than_tikvs_response_cap() {
    let Some(store) = test_store().await else {
        return;
    };
    let prefix = unique_prefix();
    let written = write_mebibyte_values(&store, &prefix, 24).await;

    let result = store.range(prefix_query(&prefix)).await.unwrap();
    assert_eq!(result.kvs.len(), written.len());
    for (n, (kv, (name, revision))) in result.kvs.iter().zip(&written).enumerate() {
        assert_eq!(&kv.key, name);
        assert_eq!(kv.mod_revision, *revision);
        assert_eq!(kv.value, vec![n as u8; 1024 * 1024]);
    }
}

#[tokio::test]
async fn delete_range_larger_than_tikvs_response_cap() {
    let Some(store) = test_store().await else {
        return;
    };
    let prefix = unique_prefix();
    write_mebibyte_values(&store, &prefix, 24).await;
    let mut range_end = prefix.clone();
    *range_end.last_mut().unwrap() += 1;

    let (deleted, _) = store.delete_range(&prefix, &range_end).await.unwrap();
    assert_eq!(deleted, 24);
    assert!(
        store
            .range(prefix_query(&prefix))
            .await
            .unwrap()
            .kvs
            .is_empty()
    );
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
    assert!(ttl_index_contains(&store, &k, pd_unix_secs(&store).await + 61).await);

    applied(&store, &k, None, put("v2")).await;
    assert_eq!(get(&store, &k).await.unwrap().lease, 0);
    assert!(!ttl_index_contains(&store, &k, pd_unix_secs(&store).await + 61).await);
}

fn ttl_put(value: &str, ttl_secs: i64) -> Write {
    Write::Put {
        value: value.as_bytes().to_vec(),
        ttl_secs: Some(ttl_secs),
    }
}

async fn ttl_index_entries(store: &Store, etcd_key: &[u8]) -> usize {
    let now = store.client().current_timestamp().await.unwrap();
    let mut snapshot = store
        .client()
        .snapshot(now, TransactionOptions::new_optimistic());
    let (start, end) = ttl_range_until(u64::MAX);
    snapshot
        .scan_keys(start..end, u32::MAX)
        .await
        .unwrap()
        .filter(|ttl_key| parse_ttl_key(ttl_key.as_ref()).map(|(_, k)| k) == Some(etcd_key))
        .count()
}

#[tokio::test]
async fn delete_removes_the_ttl_index_entry() {
    let Some(store) = test_store().await else {
        return;
    };
    let k = key(&unique_prefix(), "leased");
    applied(&store, &k, None, ttl_put("v", 3600)).await;
    assert_eq!(ttl_index_entries(&store, &k).await, 1);

    applied(&store, &k, None, Write::Delete).await;

    assert_eq!(ttl_index_entries(&store, &k).await, 0);
}

#[tokio::test]
async fn delete_range_removes_ttl_index_entries() {
    let Some(store) = test_store().await else {
        return;
    };
    let prefix = unique_prefix();
    let keys = [key(&prefix, "a"), key(&prefix, "b")];
    for k in &keys {
        applied(&store, k, None, ttl_put("v", 3600)).await;
    }

    let mut range_end = prefix.clone();
    *range_end.last_mut().unwrap() += 1;
    store.delete_range(&prefix, &range_end).await.unwrap();

    for k in &keys {
        assert_eq!(ttl_index_entries(&store, k).await, 0);
    }
}

#[tokio::test]
async fn reput_with_a_different_ttl_replaces_the_index_entry() {
    let Some(store) = test_store().await else {
        return;
    };
    let k = key(&unique_prefix(), "leased");
    applied(&store, &k, None, ttl_put("v1", 3600)).await;
    applied(&store, &k, None, ttl_put("v2", 7200)).await;

    assert_eq!(ttl_index_entries(&store, &k).await, 1);
}

#[tokio::test]
async fn delete_if_expired_deletes_only_an_expired_matching_record() {
    let Some(store) = test_store().await else {
        return;
    };
    let k = key(&unique_prefix(), "leased");
    applied(&store, &k, None, ttl_put("v", 3600)).await;
    let expiry = get(&store, &k).await.unwrap().lease as u64;

    assert!(!store.delete_if_expired(&k, expiry + 1).await.unwrap());
    assert!(get(&store, &k).await.is_some());
    assert!(store.delete_if_expired(&k, expiry).await.unwrap());
    assert!(get(&store, &k).await.is_none());
    assert_eq!(ttl_index_entries(&store, &k).await, 0);
}

#[tokio::test]
async fn delete_if_expired_drops_a_stale_index_entry_and_keeps_the_key() {
    let Some(store) = test_store().await else {
        return;
    };
    let k = key(&unique_prefix(), "leased");
    applied(&store, &k, None, ttl_put("v", 3600)).await;
    let stale_expiry = get(&store, &k).await.unwrap().lease as u64;
    applied(&store, &k, None, put("v2")).await;
    let mut txn = store.client().begin_optimistic().await.unwrap();
    txn.put(tikvd::keys::ttl_key(stale_expiry, &k), Vec::new())
        .await
        .unwrap();
    txn.commit().await.unwrap();
    assert_eq!(ttl_index_entries(&store, &k).await, 1);

    assert!(!store.delete_if_expired(&k, stale_expiry).await.unwrap());

    assert_eq!(ttl_index_entries(&store, &k).await, 0);
    assert_eq!(get(&store, &k).await.unwrap().value, b"v2");
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

async fn locks_left_on(store: &Store, raw_key: &[u8]) -> usize {
    let now = store.client().current_timestamp().await.unwrap();
    let end = [raw_key, &[0]].concat();
    store
        .client()
        .scan_locks(&now, raw_key.to_vec()..end, 16)
        .await
        .unwrap()
        .len()
}

#[tokio::test]
async fn a_leased_write_has_committed_its_data_key_when_it_returns() {
    let Some(store) = test_store().await else {
        return;
    };
    let k = key(&unique_prefix(), "leased");
    let mut data_key_still_locked = 0;
    for n in 0..40 {
        let write = if n % 4 == 3 {
            Write::Delete
        } else {
            ttl_put("v", 3600 + n)
        };
        applied(&store, &k, None, write).await;
        data_key_still_locked += locks_left_on(&store, &data_key(&k)).await;
    }
    assert_eq!(
        data_key_still_locked, 0,
        "the data key was a secondary committed after the write returned"
    );
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
