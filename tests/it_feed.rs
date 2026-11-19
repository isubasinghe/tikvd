use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tikv_client::proto::kvrpcpb::{Context, Mutation, Op, PrewriteRequest};
use tikv_client::proto::tikvpb::tikv_client::TikvClient;
use tikv_client::{Key, Timestamp, TimestampExt};
use tikvd::cdc::ChangeEvent;
use tikvd::cdc::feed::{FeedConfig, FeedOutput, spawn_feed};
use tikvd::keys::data_key;
use tikvd::record::Record;
use tikvd::store::{Store, Write, WriteOutcome};
use tokio::sync::mpsc::Receiver;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::time::{Instant, timeout_at};

static WATCHED_RANGE: RwLock<()> = RwLock::const_new(());

async fn alongside_other_feeds() -> RwLockReadGuard<'static, ()> {
    WATCHED_RANGE.read().await
}

async fn holding_the_watched_range_alone() -> RwLockWriteGuard<'static, ()> {
    WATCHED_RANGE.write().await
}

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
    format!("it-feed-{}-{nanos}-{n}/", std::process::id()).into_bytes()
}

fn key(prefix: &[u8], suffix: &str) -> Vec<u8> {
    [prefix, suffix.as_bytes()].concat()
}

fn put(value: &str) -> Write {
    Write::Put {
        value: value.as_bytes().to_vec(),
        ttl_secs: None,
    }
}

async fn applied(store: &Store, key: &[u8], write: Write) -> i64 {
    match store.write(key, None, write).await.unwrap() {
        WriteOutcome::Applied { revision, .. } => revision,
        rejected => panic!("expected applied, got {rejected:?}"),
    }
}

fn with_threshold(stalled_lock_threshold: Duration) -> FeedConfig {
    FeedConfig {
        stalled_lock_threshold,
        ..FeedConfig::default()
    }
}

async fn next_events(rx: &mut Receiver<FeedOutput>, deadline: Instant) -> (Vec<ChangeEvent>, i64) {
    match timeout_at(deadline, rx.recv()).await {
        Ok(Some(FeedOutput::Events { events, watermark })) => (events, watermark),
        Ok(Some(FeedOutput::Reset)) => panic!("unexpected reset"),
        Ok(None) => panic!("feed closed"),
        Err(_) => panic!("timed out waiting for feed output"),
    }
}

async fn first_watermark(rx: &mut Receiver<FeedOutput>) -> i64 {
    next_events(rx, Instant::now() + Duration::from_secs(10))
        .await
        .1
}

async fn collect_until(
    rx: &mut Receiver<FeedOutput>,
    prefix: &[u8],
    until_watermark: i64,
    last_watermark: i64,
) -> Vec<ChangeEvent> {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut ours = Vec::new();
    let mut watermark = last_watermark;
    let mut last_revision = 0;
    while watermark < until_watermark {
        let (events, next) = next_events(rx, deadline).await;
        assert!(
            next >= watermark,
            "watermark went back: {watermark} -> {next}"
        );
        for event in events {
            assert!(event.revision <= next, "event above watermark");
            assert!(
                event.revision > watermark,
                "event at or below a released watermark"
            );
            assert!(
                event.revision >= last_revision,
                "events out of commit order"
            );
            last_revision = event.revision;
            if event.key.starts_with(prefix) {
                ours.push(event);
            }
        }
        watermark = next;
    }
    ours
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn sees_writes_in_commit_order() {
    let Some(store) = test_store().await else {
        return;
    };
    let _shared = alongside_other_feeds().await;
    let prefix = unique_prefix();
    let (mut rx, _health) = spawn_feed(store.clone(), FeedConfig::default());
    let start = first_watermark(&mut rx).await;

    let writers: Vec<_> = (0..5)
        .map(|writer| {
            let store = store.clone();
            let prefix = prefix.clone();
            tokio::spawn(async move {
                let mut written = Vec::new();
                for i in 0..10 {
                    let k = key(&prefix, &format!("{writer}-{i}"));
                    let revision = applied(&store, &k, put("v")).await;
                    written.push((k, revision));
                }
                written
            })
        })
        .collect();
    let mut expected = Vec::new();
    for writer in writers {
        expected.extend(writer.await.unwrap());
    }
    let newest = expected.iter().map(|(_, r)| *r).max().unwrap();

    let ours = collect_until(&mut rx, &prefix, newest, start).await;

    let revisions: Vec<i64> = ours.iter().map(|e| e.revision).collect();
    assert!(
        revisions.windows(2).all(|pair| pair[0] < pair[1]),
        "revisions not strictly increasing: {revisions:?}"
    );
    let mut seen: Vec<(Vec<u8>, i64)> = ours.into_iter().map(|e| (e.key, e.revision)).collect();
    seen.sort();
    expected.sort();
    assert_eq!(seen, expected);
}

#[tokio::test]
async fn watermark_advances_when_idle() {
    let Some(store) = test_store().await else {
        return;
    };
    let _shared = alongside_other_feeds().await;
    let deadline = Instant::now() + Duration::from_secs(2);
    let (mut rx, _health) = spawn_feed(store, FeedConfig::default());
    let (_, first) = next_events(&mut rx, deadline).await;
    let (_, second) = next_events(&mut rx, deadline).await;
    assert!(second > first, "{first} then {second}");
}

#[tokio::test]
async fn create_update_delete_shapes() {
    let Some(store) = test_store().await else {
        return;
    };
    let _shared = alongside_other_feeds().await;
    let k = key(&unique_prefix(), "shape");
    let (mut rx, _health) = spawn_feed(store.clone(), FeedConfig::default());
    let start = first_watermark(&mut rx).await;

    let created = applied(&store, &k, put("v1")).await;
    let updated = applied(&store, &k, put("v2")).await;
    let deleted = applied(&store, &k, Write::Delete).await;

    let ours = collect_until(&mut rx, &k, deleted, start).await;
    let by_revision: HashMap<i64, &ChangeEvent> = ours.iter().map(|e| (e.revision, e)).collect();
    assert_eq!(ours.len(), 3, "{ours:?}");

    let create = by_revision[&created];
    assert_eq!(create.previous, None);
    assert_eq!(create.current.as_ref().unwrap().value, b"v1");

    let update = by_revision[&updated];
    assert_eq!(update.previous.as_ref().unwrap().value, b"v1");
    assert_eq!(update.current.as_ref().unwrap().value, b"v2");
    assert_eq!(update.current.as_ref().unwrap().version, 2);

    let delete = by_revision[&deleted];
    assert_eq!(delete.current, None);
    assert_eq!(delete.previous.as_ref().unwrap().value, b"v2");
}

#[tokio::test]
async fn serving_flips_true_after_first_watermark() {
    let Some(store) = test_store().await else {
        return;
    };
    let _shared = alongside_other_feeds().await;
    let (mut rx, health) = spawn_feed(store, FeedConfig::default());
    assert!(!health.is_serving());
    first_watermark(&mut rx).await;
    assert!(health.is_serving());
}

async fn leave_prewrite_lock(store: &Store, raw_key: Vec<u8>) -> i64 {
    let start_version = store.now().await.unwrap() as u64;
    let route = store
        .client()
        .regions_in_range(raw_key.clone()..[raw_key.as_slice(), &[0]].concat())
        .await
        .unwrap()
        .remove(0);
    let mut tikv = TikvClient::connect(format!("http://{}", route.store_address))
        .await
        .unwrap();
    let value = Record {
        version: 1,
        created: start_version,
        lease_expiry: 0,
        value: b"abandoned".to_vec(),
    }
    .encode();
    let request = PrewriteRequest {
        context: Some(Context {
            region_id: route.region.id,
            region_epoch: route.region.region_epoch.clone(),
            peer: Some(route.leader.clone()),
            ..Default::default()
        }),
        mutations: vec![Mutation {
            op: Op::Put as i32,
            key: raw_key.clone(),
            value,
            ..Default::default()
        }],
        primary_lock: raw_key,
        start_version,
        lock_ttl: 60_000,
        txn_size: 1,
        ..Default::default()
    };
    let response = tikv.kv_prewrite(request).await.unwrap().into_inner();
    assert!(response.region_error.is_none(), "{response:?}");
    assert!(response.errors.is_empty(), "{response:?}");
    start_version as i64
}

async fn clean_lock(store: &Store, raw_key: Vec<u8>) {
    let now = Timestamp::from_version(store.now().await.unwrap() as u64);
    let end = [raw_key.as_slice(), &[0]].concat();
    store
        .client()
        .cleanup_locks(Key::from(raw_key)..Key::from(end), &now, Default::default())
        .await
        .unwrap();
}

async fn abandoned_lock_scenario(store: Store, raw_key: Vec<u8>) {
    let (mut held, _health) = spawn_feed(store.clone(), with_threshold(Duration::from_secs(3600)));
    first_watermark(&mut held).await;
    let lock_ts = leave_prewrite_lock(&store, raw_key).await;

    let premise_until = Instant::now() + Duration::from_secs(5);
    loop {
        match timeout_at(premise_until, held.recv()).await {
            Ok(Some(FeedOutput::Events { watermark, .. })) => {
                assert!(
                    watermark <= lock_ts,
                    "watermark {watermark} passed the lock at {lock_ts}"
                );
            }
            Ok(other) => panic!("unexpected {other:?}"),
            Err(_) => break,
        }
    }
    drop(held);

    let (mut cleaning, _health) = spawn_feed(store, with_threshold(Duration::from_secs(1)));
    let (_, watermark) = next_events(&mut cleaning, Instant::now() + Duration::from_secs(10)).await;
    assert!(watermark > lock_ts);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abandoned_lock_does_not_stall_forever() {
    let Some(store) = test_store().await else {
        return;
    };
    let _alone = holding_the_watched_range_alone().await;
    let raw_key = data_key(&key(&unique_prefix(), "abandoned"));
    let outcome = tokio::spawn(abandoned_lock_scenario(store.clone(), raw_key.clone())).await;
    clean_lock(&store, raw_key).await;
    if let Err(failure) = outcome {
        std::panic::resume_unwind(failure.into_panic());
    }
}
