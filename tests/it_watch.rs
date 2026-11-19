mod common;

use std::collections::HashMap;
use std::time::Duration;

use common::{start_tikvd, unique_prefix};
use etcd_client::{
    Client, DeleteOptions, Error, EventType, GetOptions, WatchFilterType, WatchOptions,
    WatchResponse, WatchStream, Watcher,
};
use tokio::time::{Instant, timeout_at};
use tonic::Code;

const DEADLINE: Duration = Duration::from_secs(20);

async fn next_response(stream: &mut WatchStream, deadline: Instant) -> WatchResponse {
    timeout_at(deadline, stream.message())
        .await
        .expect("a watch response before the deadline")
        .expect("the watch stream is healthy")
        .expect("the watch stream is open")
}

async fn responses_with_events(stream: &mut WatchStream, events: usize) -> Vec<WatchResponse> {
    let deadline = Instant::now() + DEADLINE;
    let mut responses = Vec::new();
    let mut seen = 0;
    while seen < events {
        let response = next_response(stream, deadline).await;
        assert!(!response.canceled(), "watch cancelled: {response:?}");
        seen += response.events().len();
        if !response.events().is_empty() {
            responses.push(response);
        }
    }
    assert_eq!(seen, events, "more events than expected");
    responses
}

async fn watch_prefix(
    client: &mut Client,
    prefix: &str,
    options: WatchOptions,
) -> (Watcher, WatchStream) {
    client
        .watch(prefix, Some(options.with_prefix()))
        .await
        .unwrap()
}

async fn wait_until_serving_watches(client: &mut Client, prefix: &str) {
    watch_prefix(client, prefix, WatchOptions::new()).await;
}

fn revision_of(header: Option<&etcd_client::ResponseHeader>) -> i64 {
    header.expect("response carries a header").revision()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_then_watch_has_no_gap_or_overlap() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let prefix = unique_prefix("it-watch");
    wait_until_serving_watches(&mut client, &prefix).await;
    let writer = tokio::spawn({
        let mut client = client.clone();
        let prefix = prefix.clone();
        async move {
            for n in 0..200 {
                client
                    .put(format!("{prefix}{n:03}"), "v", None)
                    .await
                    .unwrap();
            }
        }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let listed = client
        .get(prefix.as_str(), Some(GetOptions::new().with_prefix()))
        .await
        .unwrap();
    let r = revision_of(listed.header());
    let (_watcher, mut stream) = watch_prefix(
        &mut client,
        &prefix,
        WatchOptions::new().with_start_revision(r + 1),
    )
    .await;
    writer.await.unwrap();

    let mut seen: HashMap<Vec<u8>, i64> = listed
        .kvs()
        .iter()
        .map(|kv| {
            assert!(kv.mod_revision() <= r);
            (kv.key().to_vec(), kv.mod_revision())
        })
        .collect();
    assert!(
        seen.len() < 200,
        "the list should run while the writer is still writing"
    );
    let watched = responses_with_events(&mut stream, 200 - seen.len()).await;
    for event in watched.iter().flat_map(|response| response.events()) {
        let kv = event.kv().unwrap();
        assert!(kv.mod_revision() > r);
        let previous = seen.insert(kv.key().to_vec(), kv.mod_revision());
        assert!(previous.is_none(), "{:?} seen twice", kv.key_str());
    }
    assert_eq!(seen.len(), 200);
}

#[tokio::test]
async fn create_vs_modify() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let prefix = unique_prefix("it-watch");
    let key = format!("{prefix}a");
    let (_watcher, mut stream) = watch_prefix(&mut client, &prefix, WatchOptions::new()).await;

    let created = client.put(key.as_str(), "v1", None).await.unwrap();
    let modified = client.put(key.as_str(), "v2", None).await.unwrap();

    let responses = responses_with_events(&mut stream, 2).await;
    let events: Vec<_> = responses.iter().flat_map(|r| r.events()).collect();
    let first = events[0].kv().unwrap();
    let second = events[1].kv().unwrap();
    assert_eq!(first.mod_revision(), revision_of(created.header()));
    assert_eq!(first.create_revision(), first.mod_revision());
    assert_eq!(first.version(), 1);
    assert_eq!(second.mod_revision(), revision_of(modified.header()));
    assert_ne!(second.create_revision(), second.mod_revision());
    assert_eq!(second.version(), 2);
    assert_eq!(second.value(), b"v2");
}

#[tokio::test]
async fn delete_event_carries_prev_kv() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let prefix = unique_prefix("it-watch");
    let key = format!("{prefix}a");
    client.put(key.as_str(), "v1", None).await.unwrap();
    let (_watcher, mut stream) =
        watch_prefix(&mut client, &prefix, WatchOptions::new().with_prev_key()).await;

    let deleted = client.delete(key.as_str(), None).await.unwrap();

    let responses = responses_with_events(&mut stream, 1).await;
    let event = &responses[0].events()[0];
    assert_eq!(event.event_type(), EventType::Delete);
    let kv = event.kv().unwrap();
    assert_eq!(kv.key(), key.as_bytes());
    assert_eq!(kv.mod_revision(), revision_of(deleted.header()));
    let prev = event.prev_kv().expect("prev_kv was requested");
    assert_eq!(prev.value(), b"v1");
    assert_eq!(prev.key(), key.as_bytes());
}

#[tokio::test]
async fn progress_request_returns_watermark_header() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let prefix = unique_prefix("it-watch");
    let (mut watcher, mut stream) = watch_prefix(&mut client, &prefix, WatchOptions::new()).await;
    let deadline = Instant::now() + DEADLINE;

    watcher.request_progress().await.unwrap();
    let first = next_response(&mut stream, deadline).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    watcher.request_progress().await.unwrap();
    let second = next_response(&mut stream, deadline).await;

    for response in [&first, &second] {
        assert!(response.events().is_empty());
        assert!(!response.canceled());
    }
    let first = revision_of(first.header());
    let second = revision_of(second.header());
    assert!(first > 0);
    assert!(second >= first, "{second} < {first}");
}

#[tokio::test]
async fn watch_from_ancient_revision_is_cancelled_compacted() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let prefix = unique_prefix("it-watch");
    let (_watcher, mut stream) = watch_prefix(
        &mut client,
        &prefix,
        WatchOptions::new().with_start_revision(1),
    )
    .await;

    let response = next_response(&mut stream, Instant::now() + DEADLINE).await;
    assert!(response.canceled());
    assert!(response.compact_revision() > 1);
}

#[tokio::test]
async fn range_delete_arrives_as_one_response() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let prefix = unique_prefix("it-watch");
    for name in ["a", "b", "c"] {
        client
            .put(format!("{prefix}{name}"), "v", None)
            .await
            .unwrap();
    }
    let (_watcher, mut stream) = watch_prefix(&mut client, &prefix, WatchOptions::new()).await;

    let deleted = client
        .delete(prefix.as_str(), Some(DeleteOptions::new().with_prefix()))
        .await
        .unwrap();
    assert_eq!(deleted.deleted(), 3);

    let responses = responses_with_events(&mut stream, 3).await;
    assert_eq!(responses.len(), 1, "{responses:?}");
    let revision = revision_of(deleted.header());
    for event in responses[0].events() {
        assert_eq!(event.event_type(), EventType::Delete);
        assert_eq!(event.kv().unwrap().mod_revision(), revision);
    }
}

#[tokio::test]
async fn cancel_request_is_acknowledged() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let prefix = unique_prefix("it-watch");
    let (mut watcher, mut stream) = watch_prefix(&mut client, &prefix, WatchOptions::new()).await;

    watcher.cancel().await.unwrap();

    let response = next_response(&mut stream, Instant::now() + DEADLINE).await;
    assert!(response.canceled());
    assert_eq!(response.watch_id(), watcher.watch_id());
}

#[tokio::test]
async fn filters_are_unimplemented() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let prefix = unique_prefix("it-watch");
    let options = WatchOptions::new()
        .with_prefix()
        .with_filters([WatchFilterType::NoDelete]);
    let Err(Error::GRpcStatus(status)) = client.watch(prefix.as_str(), Some(options)).await else {
        panic!("a watch with filters should fail");
    };
    assert_eq!(status.code(), Code::Unimplemented);
}
