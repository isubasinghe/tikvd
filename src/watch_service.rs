#![allow(clippy::result_large_err)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep_until};
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, warn};

use crate::cdc::ChangeEvent;
use crate::kv_service::{header, unsupported};
use crate::pb::etcdserverpb::watch_request::RequestUnion;
use crate::pb::etcdserverpb::watch_server::Watch;
use crate::pb::etcdserverpb::{WatchCreateRequest, WatchRequest, WatchResponse};
use crate::pb::mvccpb;
use crate::record::Record;
use crate::store::Store;
use crate::watch_hub::{Delivery, Subscription, WatchHub};

const OUTBOUND_CAPACITY: usize = 64;
const PROGRESS_INTERVAL: Duration = Duration::from_secs(5);
const STREAM_WIDE_WATCH_ID: i64 = -1;
const DUPLICATE_WATCH_ID: &str = "etcdserver: duplicate watch ID";
const FELL_BEHIND: &str = "etcdserver: watcher fell behind and was cancelled";

type Outbound = mpsc::Sender<Result<WatchResponse, Status>>;

pub struct WatchService {
    store: Store,
    hub: Arc<WatchHub>,
}

impl WatchService {
    pub fn new(store: Store, hub: Arc<WatchHub>) -> Self {
        WatchService { store, hub }
    }
}

#[tonic::async_trait]
impl Watch for WatchService {
    type WatchStream = ReceiverStream<Result<WatchResponse, Status>>;

    async fn watch(
        &self,
        request: Request<Streaming<WatchRequest>>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let (out, responses) = mpsc::channel(OUTBOUND_CAPACITY);
        let stream = WatchStreamState {
            store: self.store.clone(),
            hub: self.hub.clone(),
            out,
            watches: HashMap::new(),
            next_watch_id: 0,
        };
        tokio::spawn(stream.run(request.into_inner()));
        Ok(Response::new(ReceiverStream::new(responses)))
    }
}

struct WatchStreamState {
    store: Store,
    hub: Arc<WatchHub>,
    out: Outbound,
    watches: HashMap<i64, RunningWatch>,
    next_watch_id: i64,
}

struct RunningWatch {
    forwarder: JoinHandle<()>,
    delivered: Arc<AtomicI64>,
}

impl WatchStreamState {
    async fn run(mut self, mut requests: Streaming<WatchRequest>) {
        loop {
            let request = tokio::select! {
                _ = self.out.closed() => break,
                request = requests.message() => request,
            };
            match request {
                Ok(Some(request)) => {
                    if let Err(status) = self.handle(request).await {
                        let _ = self.out.send(Err(status)).await;
                        break;
                    }
                }
                Ok(None) => {
                    self.out.closed().await;
                    break;
                }
                Err(_) => break,
            }
        }
        for watch in self.watches.values() {
            watch.forwarder.abort();
        }
    }

    async fn handle(&mut self, request: WatchRequest) -> Result<(), Status> {
        self.watches
            .retain(|_, watch| !watch.forwarder.is_finished());
        match request.request_union {
            Some(RequestUnion::CreateRequest(create)) => self.create(create).await,
            Some(RequestUnion::CancelRequest(cancel)) => {
                debug!(rpc = "watch_cancel", watch_id = cancel.watch_id);
                self.cancel(cancel.watch_id).await;
                Ok(())
            }
            Some(RequestUnion::ProgressRequest(_)) => {
                debug!(rpc = "watch_progress");
                self.progress().await;
                Ok(())
            }
            None => Err(unsupported("watch request without a request")),
        }
    }

    async fn create(&mut self, create: WatchCreateRequest) -> Result<(), Status> {
        debug!(
            rpc = "watch_create",
            key = %show(&create.key),
            range_end = %show(&create.range_end),
            start_revision = create.start_revision,
            progress_notify = create.progress_notify,
            prev_kv = create.prev_kv,
            watch_id = create.watch_id,
        );
        if !create.filters.is_empty() {
            return Err(unsupported("watch with filters"));
        }
        if create.start_revision < 0 {
            return Err(unsupported("watch with a negative start revision"));
        }
        let Some(watch_id) = self.assign_watch_id(create.watch_id) else {
            self.send(WatchResponse {
                header: Some(header(self.hub.watermark())),
                watch_id: create.watch_id,
                created: true,
                canceled: true,
                cancel_reason: DUPLICATE_WATCH_ID.to_owned(),
                ..Default::default()
            })
            .await;
            return Ok(());
        };
        tokio::select! {
            _ = self.hub.wait_for_watermark() => {}
            _ = self.out.closed() => return Ok(()),
        }
        let after_revision = match create.start_revision {
            0 => match self.store.now().await {
                Ok(now) => now,
                Err(error) => {
                    warn!(watch_id, %error, "watch create failed");
                    self.send(WatchResponse {
                        header: Some(header(self.hub.watermark())),
                        watch_id,
                        created: true,
                        canceled: true,
                        cancel_reason: error.to_string(),
                        ..Default::default()
                    })
                    .await;
                    return Ok(());
                }
            },
            start => start - 1,
        };
        let subscribed = self
            .hub
            .subscribe(create.key, create.range_end, after_revision);
        let start_point = created_revision(after_revision, self.hub.watermark());
        self.send(WatchResponse {
            header: Some(header(start_point)),
            watch_id,
            created: true,
            ..Default::default()
        })
        .await;
        let Ok(subscription) = subscribed else {
            debug!(
                watch_id,
                after_revision, "watch start is outside the window, cancelled as compacted"
            );
            self.send(compacted_response(watch_id, start_point, &self.hub))
                .await;
            return Ok(());
        };
        let delivered = Arc::new(AtomicI64::new(after_revision));
        let forwarder = Forwarder {
            watch_id,
            prev_kv: create.prev_kv,
            progress_notify: create.progress_notify,
            out: self.out.clone(),
            hub: self.hub.clone(),
            progress: WatchProgress::starting_after(after_revision),
            delivered: delivered.clone(),
        };
        self.watches.insert(
            watch_id,
            RunningWatch {
                forwarder: tokio::spawn(forwarder.run(subscription)),
                delivered,
            },
        );
        Ok(())
    }

    fn assign_watch_id(&mut self, requested: i64) -> Option<i64> {
        if requested != 0 {
            return (!self.watches.contains_key(&requested)).then_some(requested);
        }
        while self.watches.contains_key(&self.next_watch_id) {
            self.next_watch_id += 1;
        }
        let assigned = self.next_watch_id;
        self.next_watch_id += 1;
        Some(assigned)
    }

    async fn cancel(&mut self, watch_id: i64) {
        let Some(watch) = self.watches.remove(&watch_id) else {
            return;
        };
        watch.forwarder.abort();
        let cut_short = watch
            .forwarder
            .await
            .is_err_and(|error| error.is_cancelled());
        if cut_short {
            self.send(WatchResponse {
                header: Some(header(watch.delivered.load(Ordering::SeqCst))),
                watch_id,
                canceled: true,
                ..Default::default()
            })
            .await;
        }
    }

    async fn progress(&self) {
        let reported = self
            .watches
            .values()
            .filter(|watch| !watch.forwarder.is_finished())
            .map(|watch| watch.delivered.load(Ordering::SeqCst));
        let revision = stream_progress(reported, self.hub.watermark());
        self.send(progress_response(STREAM_WIDE_WATCH_ID, revision))
            .await;
    }

    async fn send(&self, response: WatchResponse) {
        let _ = self.out.send(Ok(response)).await;
    }
}

struct Forwarder {
    watch_id: i64,
    prev_kv: bool,
    progress_notify: bool,
    out: Outbound,
    hub: Arc<WatchHub>,
    progress: WatchProgress,
    delivered: Arc<AtomicI64>,
}

impl Forwarder {
    async fn run(mut self, mut subscription: Subscription) {
        let mut last_sent = Instant::now();
        loop {
            let delivery = tokio::select! {
                delivery = subscription.next() => delivery,
                _ = sleep_until(last_sent + PROGRESS_INTERVAL), if self.progress_notify => {
                    let revision = self.progress.reported();
                    if !self.send(progress_response(self.watch_id, revision)).await {
                        return;
                    }
                    last_sent = Instant::now();
                    continue;
                }
            };
            match delivery {
                Delivery::Events { events, watermark } => {
                    let revision = self.progress.advance(watermark);
                    if !self.send(self.events_response(events, revision)).await {
                        return;
                    }
                    self.delivered.store(revision, Ordering::SeqCst);
                    last_sent = Instant::now();
                }
                Delivery::Progress { watermark } => {
                    let revision = self.progress.advance(watermark);
                    self.delivered.store(revision, Ordering::SeqCst);
                }
                Delivery::Cancelled { compacted } => {
                    let revision = self.progress.reported();
                    let response = if compacted {
                        compacted_response(self.watch_id, revision, &self.hub)
                    } else {
                        WatchResponse {
                            header: Some(header(revision)),
                            watch_id: self.watch_id,
                            canceled: true,
                            cancel_reason: FELL_BEHIND.to_owned(),
                            ..Default::default()
                        }
                    };
                    self.send(response).await;
                    return;
                }
            }
        }
    }

    fn events_response(&self, events: Vec<ChangeEvent>, revision: i64) -> WatchResponse {
        WatchResponse {
            header: Some(header(revision)),
            watch_id: self.watch_id,
            events: events
                .into_iter()
                .map(|event| to_proto_event(event, self.prev_kv))
                .collect(),
            ..Default::default()
        }
    }

    async fn send(&self, response: WatchResponse) -> bool {
        self.out.send(Ok(response)).await.is_ok()
    }
}

fn progress_response(watch_id: i64, revision: i64) -> WatchResponse {
    WatchResponse {
        header: Some(header(revision)),
        watch_id,
        ..Default::default()
    }
}

fn compacted_response(watch_id: i64, revision: i64, hub: &WatchHub) -> WatchResponse {
    WatchResponse {
        header: Some(header(revision)),
        watch_id,
        canceled: true,
        compact_revision: hub.oldest_revision(),
        ..Default::default()
    }
}

fn to_proto_event(event: ChangeEvent, prev_kv: bool) -> mvccpb::Event {
    let (kind, kv) = match event.current {
        Some(current) => {
            let create_revision = if current.version == 1 {
                event.revision
            } else {
                current.created as i64
            };
            (
                mvccpb::event::EventType::Put,
                key_value(event.key.clone(), current, create_revision, event.revision),
            )
        }
        None => (
            mvccpb::event::EventType::Delete,
            mvccpb::KeyValue {
                key: event.key.clone(),
                mod_revision: event.revision,
                ..Default::default()
            },
        ),
    };
    let prev_kv = event.previous.filter(|_| prev_kv).map(|previous| {
        let create_revision = previous.created as i64;
        key_value(event.key, previous, create_revision, 0)
    });
    mvccpb::Event {
        r#type: kind as i32,
        kv: Some(kv),
        prev_kv,
    }
}

fn key_value(
    key: Vec<u8>,
    record: Record,
    create_revision: i64,
    mod_revision: i64,
) -> mvccpb::KeyValue {
    mvccpb::KeyValue {
        key,
        create_revision,
        mod_revision,
        version: record.version,
        value: record.value,
        lease: record.lease_expiry as i64,
    }
}

fn show(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

struct WatchProgress {
    reported: i64,
}

impl WatchProgress {
    fn starting_after(after_revision: i64) -> WatchProgress {
        WatchProgress {
            reported: after_revision,
        }
    }

    fn advance(&mut self, watermark: i64) -> i64 {
        self.reported = self.reported.max(watermark);
        self.reported
    }

    fn reported(&self) -> i64 {
        self.reported
    }
}

fn created_revision(after_revision: i64, watermark: i64) -> i64 {
    after_revision.max(watermark)
}

fn stream_progress(reported: impl Iterator<Item = i64>, watermark: i64) -> i64 {
    reported.min().unwrap_or(watermark)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(version: i64, created: u64) -> Record {
        Record {
            version,
            created,
            lease_expiry: 0,
            value: b"v".to_vec(),
        }
    }

    #[test]
    fn an_update_without_a_previous_value_is_not_reported_as_a_create() {
        let event = ChangeEvent {
            key: b"a".to_vec(),
            revision: 50,
            current: Some(record(2, 10)),
            previous: None,
        };
        let kv = to_proto_event(event, false).kv.unwrap();
        assert_ne!(kv.create_revision, kv.mod_revision);
        assert_eq!(kv.create_revision, 10);
    }

    #[test]
    fn a_first_version_is_reported_as_a_create_whatever_its_previous_value() {
        let event = ChangeEvent {
            key: b"a".to_vec(),
            revision: 50,
            current: Some(record(1, 10)),
            previous: Some(record(3, 5)),
        };
        let kv = to_proto_event(event, false).kv.unwrap();
        assert_eq!(kv.create_revision, kv.mod_revision);
    }

    #[test]
    fn progress_is_never_below_the_watch_start() {
        let mut progress = WatchProgress::starting_after(105);
        assert!(progress.reported() >= 105);
        assert!(progress.advance(102) >= 105);
        assert!(progress.advance(104) >= 105);
        assert!(progress.reported() >= 105);
        assert_eq!(progress.advance(110), 110);
    }

    #[test]
    fn reported_revision_never_decreases() {
        let mut progress = WatchProgress::starting_after(10);
        let mut last = progress.reported();
        for watermark in [12, 30, 20, 30, 25, 40, 5] {
            let reported = progress.advance(watermark);
            assert!(reported >= last, "{reported} after {last}");
            last = reported;
        }
        assert_eq!(last, 40);
    }

    #[test]
    fn stream_progress_is_the_minimum_over_watches_each_clamped_to_its_start() {
        let mut late = WatchProgress::starting_after(105);
        let mut early = WatchProgress::starting_after(50);
        late.advance(100);
        early.advance(100);
        assert_eq!(
            stream_progress([late.reported(), early.reported()].into_iter(), 100),
            100
        );
        late.advance(120);
        early.advance(120);
        assert_eq!(
            stream_progress([late.reported(), early.reported()].into_iter(), 120),
            120
        );
        assert_eq!(stream_progress(std::iter::empty(), 120), 120);
    }

    #[test]
    fn created_header_is_the_start_point_for_a_revision_zero_watch() {
        let now = 130;
        assert_eq!(created_revision(now, 100), 130);
        assert_eq!(created_revision(40, 100), 100);
    }

    #[test]
    fn events_header_is_not_below_the_watch_start() {
        let mut progress = WatchProgress::starting_after(105);
        assert_eq!(progress.advance(103), 105);
        assert_eq!(progress.advance(108), 108);
    }
}
