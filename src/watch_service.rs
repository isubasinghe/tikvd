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
use tracing::debug;

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
            0 => self
                .store
                .now()
                .await
                .map_err(|error| Status::unavailable(error.to_string()))?,
            start => start - 1,
        };
        let subscribed = self
            .hub
            .subscribe(create.key, create.range_end, after_revision);
        let watermark = self.hub.watermark();
        self.send(WatchResponse {
            header: Some(header(watermark)),
            watch_id,
            created: true,
            ..Default::default()
        })
        .await;
        let Ok(subscription) = subscribed else {
            self.send(compacted_response(watch_id, watermark, &self.hub))
                .await;
            return Ok(());
        };
        let delivered = Arc::new(AtomicI64::new(after_revision.min(watermark)));
        let forwarder = Forwarder {
            watch_id,
            prev_kv: create.prev_kv,
            progress_notify: create.progress_notify,
            out: self.out.clone(),
            hub: self.hub.clone(),
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
        let revision = self
            .watches
            .values()
            .filter(|watch| !watch.forwarder.is_finished())
            .map(|watch| watch.delivered.load(Ordering::SeqCst))
            .min()
            .unwrap_or_else(|| self.hub.watermark());
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
    delivered: Arc<AtomicI64>,
}

impl Forwarder {
    async fn run(self, mut subscription: Subscription) {
        let mut last_sent = Instant::now();
        loop {
            let delivery = tokio::select! {
                delivery = subscription.next() => delivery,
                _ = sleep_until(last_sent + PROGRESS_INTERVAL), if self.progress_notify => {
                    let revision = self.delivered.load(Ordering::SeqCst);
                    if !self.send(progress_response(self.watch_id, revision)).await {
                        return;
                    }
                    last_sent = Instant::now();
                    continue;
                }
            };
            match delivery {
                Delivery::Events { events, watermark } => {
                    if !self.send(self.events_response(events, watermark)).await {
                        return;
                    }
                    self.delivered.store(watermark, Ordering::SeqCst);
                    last_sent = Instant::now();
                }
                Delivery::Progress { watermark } => {
                    self.delivered.store(watermark, Ordering::SeqCst);
                }
                Delivery::Cancelled { compacted } => {
                    let revision = self.delivered.load(Ordering::SeqCst);
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

    fn events_response(&self, events: Vec<ChangeEvent>, watermark: i64) -> WatchResponse {
        WatchResponse {
            header: Some(header(watermark)),
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
    let created_here = event.previous.is_none();
    let (kind, kv) = match event.current {
        Some(current) => {
            let create_revision = if created_here {
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
