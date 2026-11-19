use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tikv_client::proto::cdcpb::change_data_client::ChangeDataClient;
use tikv_client::proto::cdcpb::{ChangeDataEvent, ChangeDataRequest};
use tikv_client::{Backoff, BoundRange, Key, RegionRoute, Timestamp, TimestampExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};
use tonic::codegen::tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Endpoint;
use tracing::{debug, info, warn};

use super::plan::KeyRange;
pub use super::session::FeedOutput;
use super::session::{Action, Session, SessionConfig, retry_delay};
use crate::store::Store;

const OUTPUT_CAPACITY: usize = 64;
const INCOMING_CAPACITY: usize = 1024;
const TICK: Duration = Duration::from_millis(100);
const LOCK_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_MESSAGE_BYTES: usize = 512 << 20;
const LOCK_SCAN_BATCH: u32 = 1024;

#[derive(Debug, Clone)]
pub struct FeedConfig {
    pub stalled_lock_threshold: Duration,
    pub unhealthy_after: Duration,
    pub sorter_memory_limit: usize,
}

impl Default for FeedConfig {
    fn default() -> Self {
        Self {
            stalled_lock_threshold: Duration::from_secs(5),
            unhealthy_after: Duration::from_secs(30),
            sorter_memory_limit: 256 << 20,
        }
    }
}

#[derive(Clone)]
pub struct FeedHealth {
    last_advance: Arc<Mutex<Option<Instant>>>,
    unhealthy_after: Duration,
}

impl FeedHealth {
    fn new(unhealthy_after: Duration) -> Self {
        Self {
            last_advance: Arc::new(Mutex::new(None)),
            unhealthy_after,
        }
    }

    pub fn is_serving(&self) -> bool {
        self.last_advance
            .lock()
            .unwrap()
            .is_some_and(|at| at.elapsed() <= self.unhealthy_after)
    }

    fn advanced(&self) {
        *self.last_advance.lock().unwrap() = Some(Instant::now());
    }

    fn lost(&self) {
        *self.last_advance.lock().unwrap() = None;
    }
}

pub fn spawn_feed(store: Store, config: FeedConfig) -> (mpsc::Receiver<FeedOutput>, FeedHealth) {
    let (out, receiver) = mpsc::channel(OUTPUT_CAPACITY);
    let health = FeedHealth::new(config.unhealthy_after);
    tokio::spawn(run_feed(store, config, out, health.clone()));
    (receiver, health)
}

fn bound(range: &KeyRange) -> BoundRange {
    (Key::from(range.0.clone())..Key::from(range.1.clone())).into()
}

enum Ended {
    Closed,
    Reset,
}

enum Incoming {
    Message {
        stream: u64,
        event: ChangeDataEvent,
    },
    Closed {
        stream: u64,
        error: String,
    },
    LookedUp {
        range: KeyRange,
        attempt: u32,
        routes: Result<Vec<RegionRoute>, String>,
    },
    Clock(Option<u64>),
    CleanupDone,
}

struct StreamHandle {
    requests: mpsc::UnboundedSender<ChangeDataRequest>,
    reader: JoinHandle<()>,
}

impl Drop for StreamHandle {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

async fn run_feed(
    store: Store,
    config: FeedConfig,
    out: mpsc::Sender<FeedOutput>,
    health: FeedHealth,
) {
    loop {
        let Some(floor) = start_timestamp(&store, &out).await else {
            return;
        };
        info!(checkpoint_ts = floor, "cdc feed starting");
        let session_config = SessionConfig {
            floor,
            cluster_id: store.client().cluster_id().await,
            sorter_memory_limit: config.sorter_memory_limit,
            stalled_lock_threshold: config.stalled_lock_threshold,
        };
        let (session, first_actions) = Session::new(session_config);
        let mut runner = Runner::new(&store, &out, &health);
        match runner.run(session, first_actions).await {
            Ended::Closed => return,
            Ended::Reset => {
                health.lost();
                if out.send(FeedOutput::Reset).await.is_err() {
                    return;
                }
            }
        }
    }
}

struct Runner {
    store: Store,
    out: mpsc::Sender<FeedOutput>,
    health: FeedHealth,
    incoming: mpsc::Sender<Incoming>,
    incoming_rx: mpsc::Receiver<Incoming>,
    streams: HashMap<u64, StreamHandle>,
    cleanups_running: usize,
    clock_pending: bool,
    last_clock: Instant,
}

impl Runner {
    fn new(store: &Store, out: &mpsc::Sender<FeedOutput>, health: &FeedHealth) -> Runner {
        let (incoming, incoming_rx) = mpsc::channel(INCOMING_CAPACITY);
        Runner {
            store: store.clone(),
            out: out.clone(),
            health: health.clone(),
            incoming,
            incoming_rx,
            streams: HashMap::new(),
            cleanups_running: 0,
            clock_pending: false,
            last_clock: Instant::now(),
        }
    }

    async fn run(&mut self, mut session: Session, first_actions: Vec<Action>) -> Ended {
        if let Err(ended) = self.execute(first_actions).await {
            return ended;
        }
        let mut tick = tokio::time::interval(TICK);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            let actions = tokio::select! {
                _ = self.out.closed() => return Ended::Closed,
                Some(incoming) = self.incoming_rx.recv() => self.on_incoming(&mut session, incoming),
                _ = tick.tick() => {
                    self.request_clock();
                    session.on_tick()
                }
            };
            if let Err(ended) = self.execute(actions).await {
                return ended;
            }
        }
    }

    fn on_incoming(&mut self, session: &mut Session, incoming: Incoming) -> Vec<Action> {
        match incoming {
            Incoming::Message { stream, event } => session.on_change_data(stream, event),
            Incoming::Closed { stream, error } => {
                warn!(stream, error, "cdc stream closed");
                self.streams.remove(&stream);
                session.on_stream_closed(stream)
            }
            Incoming::LookedUp {
                range,
                attempt,
                routes,
            } => session.on_lookup(range, attempt, routes),
            Incoming::Clock(now) => {
                self.clock_pending = false;
                now.map_or_else(Vec::new, |now| session.on_clock(now))
            }
            Incoming::CleanupDone => {
                self.cleanups_running -= 1;
                Vec::new()
            }
        }
    }

    async fn execute(&mut self, actions: Vec<Action>) -> Result<(), Ended> {
        for action in actions {
            match action {
                Action::Register {
                    stream,
                    store_address,
                    request,
                } => {
                    let _ = self.stream(stream, &store_address).requests.send(request);
                }
                Action::Deregister { stream, request } => {
                    if let Some(handle) = self.streams.get(&stream) {
                        let _ = handle.requests.send(request);
                    }
                }
                Action::LookUp {
                    range,
                    attempt,
                    after,
                } => self.look_up(range, attempt, after),
                Action::CleanLocks { range, safepoint } => self.clean_locks(range, safepoint),
                Action::Emit(output) => {
                    self.out.send(output).await.map_err(|_| Ended::Closed)?;
                    self.health.advanced();
                }
                Action::Reset => return Err(Ended::Reset),
            }
        }
        Ok(())
    }

    fn stream(&mut self, stream: u64, store_address: &str) -> &StreamHandle {
        let incoming = self.incoming.clone();
        self.streams.entry(stream).or_insert_with(|| {
            let (requests, requests_rx) = mpsc::unbounded_channel();
            let reader = tokio::spawn(read_stream(
                stream,
                store_address.to_owned(),
                requests_rx,
                incoming,
            ));
            StreamHandle { requests, reader }
        })
    }

    fn look_up(&self, range: KeyRange, attempt: u32, after: Duration) {
        let store = self.store.clone();
        let incoming = self.incoming.clone();
        tokio::spawn(async move {
            tokio::time::sleep(after).await;
            let routes = lookup_regions(&store, &range)
                .await
                .map_err(|error| error.to_string());
            let _ = incoming
                .send(Incoming::LookedUp {
                    range,
                    attempt,
                    routes,
                })
                .await;
        });
    }

    fn request_clock(&mut self) {
        if self.clock_pending
            || self.cleanups_running > 0
            || self.last_clock.elapsed() < LOCK_CHECK_INTERVAL
        {
            return;
        }
        self.clock_pending = true;
        self.last_clock = Instant::now();
        let store = self.store.clone();
        let incoming = self.incoming.clone();
        tokio::spawn(async move {
            let now = match store.now().await {
                Ok(now) => Some(now as u64),
                Err(error) => {
                    warn!(%error, "fetching a timestamp for lock cleanup failed");
                    None
                }
            };
            let _ = incoming.send(Incoming::Clock(now)).await;
        });
    }

    fn clean_locks(&mut self, range: KeyRange, safepoint: u64) {
        self.cleanups_running += 1;
        let store = self.store.clone();
        let incoming = self.incoming.clone();
        tokio::spawn(async move {
            clean_stalled_locks(&store, &range, safepoint).await;
            let _ = incoming.send(Incoming::CleanupDone).await;
        });
    }
}

async fn start_timestamp(store: &Store, out: &mpsc::Sender<FeedOutput>) -> Option<u64> {
    let mut attempt = 0;
    loop {
        if out.is_closed() {
            return None;
        }
        match store.now().await {
            Ok(now) => return Some(now as u64),
            Err(error) => warn!(%error, "fetching the cdc start timestamp failed, retrying"),
        }
        attempt += 1;
        tokio::time::sleep(retry_delay(attempt)).await;
    }
}

async fn lookup_regions(
    store: &Store,
    range: &KeyRange,
) -> Result<Vec<RegionRoute>, tikv_client::Error> {
    refresh_region_cache(store, range).await;
    store.client().regions_in_range(bound(range)).await
}

async fn refresh_region_cache(store: &Store, range: &KeyRange) {
    // The client exposes no cache invalidation; a region request's error handling refreshes it.
    if let Err(error) = store
        .client()
        .scan_locks(&Timestamp::from_version(1), bound(range), 1)
        .await
    {
        debug!(%error, "region cache refresh probe failed");
    }
}

async fn clean_stalled_locks(store: &Store, range: &KeyRange, safepoint: u64) {
    if let Err(error) = resolve_expired_locks(store, range, safepoint).await {
        warn!(%error, "resolving stalled locks failed");
    }
}

async fn resolve_expired_locks(
    store: &Store,
    range: &KeyRange,
    safepoint: u64,
) -> Result<(), tikv_client::Error> {
    let safepoint = Timestamp::from_version(safepoint);
    let locks = store
        .client()
        .scan_locks(&safepoint, bound(range), LOCK_SCAN_BATCH)
        .await?;
    if locks.is_empty() {
        return Ok(());
    }
    info!(
        locks = locks.len(),
        "resolving stalled locks whose TTL has expired"
    );
    let live = store
        .client()
        .resolve_locks(locks, safepoint, Backoff::no_backoff())
        .await?;
    debug!(
        live = live.len(),
        "stalled locks still held by live transactions"
    );
    Ok(())
}

async fn read_stream(
    stream: u64,
    store_address: String,
    requests: mpsc::UnboundedReceiver<ChangeDataRequest>,
    incoming: mpsc::Sender<Incoming>,
) {
    let error = match pump_stream(stream, &store_address, requests, &incoming).await {
        Ok(()) => "stream ended".to_owned(),
        Err(error) => error.to_string(),
    };
    let _ = incoming.send(Incoming::Closed { stream, error }).await;
}

async fn pump_stream(
    stream: u64,
    store_address: &str,
    requests: mpsc::UnboundedReceiver<ChangeDataRequest>,
    incoming: &mpsc::Sender<Incoming>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // ponytail: plaintext gRPC only; a TLS cluster needs the client's security config here
    let channel = Endpoint::from_shared(format!("http://{store_address}"))?
        .connect_timeout(CONNECT_TIMEOUT)
        .http2_keep_alive_interval(KEEPALIVE_INTERVAL)
        .keep_alive_timeout(KEEPALIVE_TIMEOUT)
        .connect()
        .await?;
    let mut client = ChangeDataClient::new(channel).max_decoding_message_size(MAX_MESSAGE_BYTES);
    let mut responses = client
        .event_feed(UnboundedReceiverStream::new(requests))
        .await?
        .into_inner();
    while let Some(event) = responses.message().await? {
        if incoming
            .send(Incoming::Message { stream, event })
            .await
            .is_err()
        {
            break;
        }
    }
    Ok(())
}
