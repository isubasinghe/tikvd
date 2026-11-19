use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tikv_client::proto::cdcpb::change_data_client::ChangeDataClient;
use tikv_client::proto::cdcpb::change_data_request::{Deregister, KvApi, Register, Request};
use tikv_client::proto::cdcpb::event::{Event as RegionEventKind, LogType, Row};
use tikv_client::proto::cdcpb::{ChangeDataEvent, ChangeDataRequest, Event as RegionEvent, Header};
use tikv_client::proto::kvrpcpb::ExtraOp;
use tikv_client::proto::metapb::RegionEpoch;
use tikv_client::transaction::ResolveLocksOptions;
use tikv_client::{BoundRange, Key, RegionRoute, Timestamp, TimestampExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};
use tonic::codegen::tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Endpoint;
use tracing::{debug, info, warn};

use super::{Assembler, ChangeEvent, Sorter};
use crate::keys::data_range;
use crate::store::Store;

const OUTPUT_CAPACITY: usize = 64;
const INCOMING_CAPACITY: usize = 1024;
const TICK: Duration = Duration::from_millis(100);
const LOCK_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const FIRST_RETRY: Duration = Duration::from_millis(50);
const LONGEST_RETRY: Duration = Duration::from_secs(5);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_MESSAGE_BYTES: usize = 512 << 20;
const PHYSICAL_SHIFT: u32 = 18;
const PER_REGION_RESOLVED_TS_VERSION: &str = "";
const GROUP: usize = 8;
const GROUP_MARKER: u8 = 0xFF;

#[derive(Debug, Clone, PartialEq)]
pub enum FeedOutput {
    Events {
        events: Vec<ChangeEvent>,
        watermark: i64,
    },
    Reset,
}

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

type KeyRange = (Vec<u8>, Vec<u8>);

pub fn memcomparable_key(raw: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity((raw.len() / GROUP + 1) * (GROUP + 1));
    let mut groups = raw.chunks_exact(GROUP);
    for group in groups.by_ref() {
        encoded.extend_from_slice(group);
        encoded.push(GROUP_MARKER);
    }
    let tail = groups.remainder();
    let padding = GROUP - tail.len();
    encoded.extend_from_slice(tail);
    encoded.resize(encoded.len() + padding, 0);
    encoded.push(GROUP_MARKER - padding as u8);
    encoded
}

fn clamp_to(range: &KeyRange, region_start: &[u8], region_end: &[u8]) -> Option<KeyRange> {
    let start = range.0.as_slice().max(region_start);
    let end = match region_end {
        [] => range.1.as_slice(),
        _ => range.1.as_slice().min(region_end),
    };
    (start < end).then(|| (start.to_vec(), end.to_vec()))
}

#[derive(Debug, Clone, PartialEq)]
struct Registration {
    request_id: u64,
    stream: u64,
    initialized: bool,
}

#[derive(Debug, Clone, PartialEq)]
struct Region {
    range: KeyRange,
    epoch: RegionEpoch,
    checkpoint: u64,
    reported: Option<u64>,
    failures: u32,
    registration: Option<Registration>,
}

#[derive(Debug, Clone, PartialEq)]
struct FreshRegion {
    id: u64,
    range: KeyRange,
    epoch: RegionEpoch,
    store_address: String,
}

#[derive(Debug, PartialEq)]
struct Replacement {
    dropped: Vec<u64>,
    registered: Vec<(u64, u64)>,
}

fn plan_replacement(
    tracked: &BTreeMap<u64, Region>,
    fresh: &[FreshRegion],
    fallback_checkpoint: u64,
) -> Replacement {
    let registered = fresh
        .iter()
        .filter(|region| !is_live_at(tracked.get(&region.id), &region.epoch))
        .map(|region| {
            let checkpoint = lowest_overlapping_checkpoint(tracked, &region.range);
            (region.id, checkpoint.unwrap_or(fallback_checkpoint))
        })
        .collect();
    let fresh_ids: HashSet<u64> = fresh.iter().map(|region| region.id).collect();
    let dropped = tracked
        .iter()
        .filter(|(id, region)| !fresh_ids.contains(id) && is_covered(&region.range, fresh))
        .map(|(id, _)| *id)
        .collect();
    Replacement {
        dropped,
        registered,
    }
}

fn is_live_at(region: Option<&Region>, epoch: &RegionEpoch) -> bool {
    region.is_some_and(|region| region.registration.is_some() && region.epoch == *epoch)
}

fn lowest_overlapping_checkpoint(tracked: &BTreeMap<u64, Region>, range: &KeyRange) -> Option<u64> {
    tracked
        .values()
        .filter(|region| overlaps(&region.range, range))
        .map(|region| region.checkpoint)
        .min()
}

fn overlaps(a: &KeyRange, b: &KeyRange) -> bool {
    a.0 < b.1 && b.0 < a.1
}

fn is_covered(range: &KeyRange, fresh: &[FreshRegion]) -> bool {
    let mut ranges: Vec<&KeyRange> = fresh.iter().map(|region| &region.range).collect();
    ranges.sort();
    let mut covered_to = range.0.as_slice();
    for (start, end) in ranges {
        if start.as_slice() <= covered_to && end.as_slice() > covered_to {
            covered_to = end;
        }
    }
    covered_to >= range.1.as_slice()
}

fn reset_sorter_regions(sorter: &mut Sorter, all: &[u64], reset: &[u64]) {
    let kept: Vec<u64> = all
        .iter()
        .copied()
        .filter(|id| !reset.contains(id))
        .collect();
    sorter.set_regions(&kept);
    sorter.set_regions(all);
}

fn physical_ms(ts: u64) -> u64 {
    ts >> PHYSICAL_SHIFT
}

fn lagging(resolved: u64, now: u64, threshold: Duration) -> bool {
    let lag_ms = physical_ms(now).saturating_sub(physical_ms(resolved));
    u128::from(lag_ms) > threshold.as_millis()
}

fn cleanup_safepoint(now: u64, threshold: Duration) -> u64 {
    let threshold_ms = u64::try_from(threshold.as_millis()).unwrap_or(u64::MAX);
    physical_ms(now).saturating_sub(threshold_ms) << PHYSICAL_SHIFT
}

fn release(
    events: Vec<ChangeEvent>,
    watermark: i64,
    floor: u64,
    last_emitted: Option<i64>,
) -> Option<FeedOutput> {
    let below_start = u64::try_from(watermark).is_ok_and(|watermark| watermark < floor);
    let unmoved = last_emitted.is_some_and(|last| watermark <= last);
    if below_start || (unmoved && events.is_empty()) {
        return None;
    }
    Some(FeedOutput::Events { events, watermark })
}

fn retry_delay(attempt: u32) -> Duration {
    FIRST_RETRY
        .saturating_mul(1 << attempt.min(16))
        .min(LONGEST_RETRY)
}

fn bound(range: &KeyRange) -> BoundRange {
    (Key::from(range.0.clone())..Key::from(range.1.clone())).into()
}

fn watched_range() -> KeyRange {
    data_range(b"", b"\0")
}

enum Ended {
    Closed,
    Reset,
}

enum Incoming {
    Message(ChangeDataEvent),
    Closed {
        stream: u64,
        error: String,
    },
    Resolved {
        range: KeyRange,
        attempt: u32,
        routes: Result<Vec<RegionRoute>, tikv_client::Error>,
    },
    CleanupDone,
}

struct Pending {
    range: KeyRange,
    attempt: u32,
    due: Instant,
}

struct StreamHandle {
    id: u64,
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
        let Some(session) = Session::start(&store, &config, &out, &health).await else {
            return;
        };
        match session.run().await {
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

struct Session {
    store: Store,
    config: FeedConfig,
    out: mpsc::Sender<FeedOutput>,
    health: FeedHealth,
    incoming: mpsc::Sender<Incoming>,
    incoming_rx: mpsc::Receiver<Incoming>,
    cluster_id: u64,
    floor: u64,
    sorter: Sorter,
    assembler: Assembler,
    tracked: BTreeMap<u64, Region>,
    pending: Vec<Pending>,
    streams: HashMap<String, StreamHandle>,
    next_id: u64,
    last_emitted: Option<i64>,
    cleanup_running: bool,
    last_cleanup_check: Instant,
}

impl Session {
    async fn start(
        store: &Store,
        config: &FeedConfig,
        out: &mpsc::Sender<FeedOutput>,
        health: &FeedHealth,
    ) -> Option<Session> {
        let floor = start_timestamp(store, out).await?;
        let cluster_id = store.client().cluster_id().await;
        let (incoming, incoming_rx) = mpsc::channel(INCOMING_CAPACITY);
        info!(checkpoint_ts = floor, "cdc feed starting");
        Some(Session {
            store: store.clone(),
            config: config.clone(),
            out: out.clone(),
            health: health.clone(),
            incoming,
            incoming_rx,
            cluster_id,
            floor,
            sorter: Sorter::new(config.sorter_memory_limit),
            assembler: Assembler::new(),
            tracked: BTreeMap::new(),
            pending: vec![Pending {
                range: watched_range(),
                attempt: 0,
                due: Instant::now(),
            }],
            streams: HashMap::new(),
            next_id: 1,
            last_emitted: None,
            cleanup_running: false,
            last_cleanup_check: Instant::now(),
        })
    }

    async fn run(mut self) -> Ended {
        let mut tick = tokio::time::interval(TICK);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            let step = tokio::select! {
                _ = self.out.closed() => Err(Ended::Closed),
                Some(incoming) = self.incoming_rx.recv() => self.on_incoming(incoming),
                _ = tick.tick() => self.on_tick().await,
            };
            if let Err(ended) = step {
                return ended;
            }
        }
    }

    fn on_incoming(&mut self, incoming: Incoming) -> Result<(), Ended> {
        match incoming {
            Incoming::Message(event) => return self.on_change_data(event),
            Incoming::Closed { stream, error } => self.on_stream_closed(stream, &error),
            Incoming::Resolved {
                range,
                attempt,
                routes,
            } => self.on_resolved_routes(range, attempt, routes),
            Incoming::CleanupDone => self.cleanup_running = false,
        }
        Ok(())
    }

    async fn on_tick(&mut self) -> Result<(), Ended> {
        self.start_due_lookups();
        self.start_lock_cleanup();
        self.release().await
    }

    async fn release(&mut self) -> Result<(), Ended> {
        let (events, watermark) = self.sorter.drain();
        let Some(output) = release(events, watermark, self.floor, self.last_emitted) else {
            return Ok(());
        };
        self.out.send(output).await.map_err(|_| Ended::Closed)?;
        if self.last_emitted.is_none_or(|last| watermark > last) {
            self.last_emitted = Some(watermark);
            self.health.advanced();
        }
        Ok(())
    }

    fn on_change_data(&mut self, event: ChangeDataEvent) -> Result<(), Ended> {
        for region_event in event.events {
            self.on_region_event(region_event)?;
        }
        if let Some(resolved) = event.resolved_ts {
            for region_id in resolved.regions {
                self.on_resolved_ts(region_id, resolved.request_id, resolved.ts);
            }
        }
        Ok(())
    }

    fn on_region_event(&mut self, event: RegionEvent) -> Result<(), Ended> {
        let region_id = event.region_id;
        if self.registration(region_id, event.request_id).is_none() {
            return Ok(());
        }
        match event.event {
            Some(RegionEventKind::Entries(entries)) => {
                for row in entries.entries {
                    self.on_row(region_id, row)?;
                }
            }
            Some(RegionEventKind::Error(error)) => {
                warn!(region_id, ?error, "cdc region error, re-registering");
                self.lose_registration(region_id);
            }
            Some(RegionEventKind::ResolvedTs(ts)) => {
                self.on_resolved_ts(region_id, event.request_id, ts)
            }
            _ => {}
        }
        Ok(())
    }

    fn on_row(&mut self, region_id: u64, row: Row) -> Result<(), Ended> {
        if row.r#type == LogType::Initialized as i32 {
            if let Some(region) = self.tracked.get_mut(&region_id) {
                region.failures = 0;
                if let Some(registration) = region.registration.as_mut() {
                    registration.initialized = true;
                }
            }
            return Ok(());
        }
        match self.assembler.on_row(row) {
            Ok(Some(event)) if event.revision as u64 > self.floor => {
                self.sorter.push(event).map_err(|_| {
                    warn!("cdc sorter is full, resetting the feed");
                    Ended::Reset
                })
            }
            Ok(_) => Ok(()),
            Err(_) => {
                warn!(region_id, "skipping a cdc row whose record does not decode");
                Ok(())
            }
        }
    }

    fn on_resolved_ts(&mut self, region_id: u64, request_id: u64, ts: u64) {
        let initialized = self
            .registration(region_id, request_id)
            .is_some_and(|registration| registration.initialized);
        if !initialized {
            return;
        }
        let region = self
            .tracked
            .get_mut(&region_id)
            .expect("registration implies tracked");
        region.checkpoint = region.checkpoint.max(ts);
        region.reported = Some(ts);
        self.sorter.set_resolved(region_id, ts);
    }

    fn registration(&self, region_id: u64, request_id: u64) -> Option<&Registration> {
        self.tracked
            .get(&region_id)?
            .registration
            .as_ref()
            .filter(|registration| registration.request_id == request_id)
    }

    fn lose_registration(&mut self, region_id: u64) {
        let Some(region) = self.tracked.get_mut(&region_id) else {
            return;
        };
        region.registration = None;
        region.failures += 1;
        let (range, attempt) = (region.range.clone(), region.failures);
        self.retry_later(range, attempt);
    }

    fn retry_later(&mut self, range: KeyRange, attempt: u32) {
        self.pending.push(Pending {
            range,
            attempt,
            due: Instant::now() + retry_delay(attempt),
        });
    }

    fn on_stream_closed(&mut self, stream: u64, error: &str) {
        warn!(stream, error, "cdc stream closed");
        self.streams.retain(|_, handle| handle.id != stream);
        let lost: Vec<u64> = self
            .tracked
            .iter()
            .filter(|(_, region)| {
                region
                    .registration
                    .as_ref()
                    .is_some_and(|registration| registration.stream == stream)
            })
            .map(|(id, _)| *id)
            .collect();
        for region_id in lost {
            self.lose_registration(region_id);
        }
    }

    fn start_due_lookups(&mut self) {
        let now = Instant::now();
        let (due, waiting) = std::mem::take(&mut self.pending)
            .into_iter()
            .partition(|pending| pending.due <= now);
        self.pending = waiting;
        for Pending { range, attempt, .. } in due {
            let store = self.store.clone();
            let incoming = self.incoming.clone();
            tokio::spawn(async move {
                let routes = lookup_regions(&store, &range).await;
                let _ = incoming
                    .send(Incoming::Resolved {
                        range,
                        attempt,
                        routes,
                    })
                    .await;
            });
        }
    }

    fn on_resolved_routes(
        &mut self,
        range: KeyRange,
        attempt: u32,
        routes: Result<Vec<RegionRoute>, tikv_client::Error>,
    ) {
        let routes = match routes {
            Ok(routes) => routes,
            Err(error) => {
                warn!(?error, attempt, "looking up cdc regions failed, retrying");
                self.retry_later(range, attempt + 1);
                return;
            }
        };
        let fresh: Vec<FreshRegion> = routes
            .into_iter()
            .filter_map(|route| fresh_region(&watched_range(), route))
            .collect();
        if !is_covered(&range, &fresh) {
            warn!(attempt, "cdc region lookup left a gap, retrying");
            self.retry_later(range, attempt + 1);
            return;
        }
        self.replace_regions(&fresh, attempt);
    }

    fn replace_regions(&mut self, fresh: &[FreshRegion], attempt: u32) {
        let fallback = self
            .tracked
            .values()
            .map(|region| region.checkpoint)
            .min()
            .unwrap_or(self.floor);
        let plan = plan_replacement(&self.tracked, fresh, fallback);
        let superseded = plan
            .dropped
            .iter()
            .chain(plan.registered.iter().map(|(id, _)| id));
        for region_id in superseded {
            self.deregister(*region_id);
        }
        for region_id in &plan.dropped {
            self.tracked.remove(region_id);
        }
        for &(region_id, checkpoint) in &plan.registered {
            let region = fresh
                .iter()
                .find(|f| f.id == region_id)
                .expect("planned from fresh");
            self.tracked.insert(
                region_id,
                Region {
                    range: region.range.clone(),
                    epoch: region.epoch.clone(),
                    checkpoint,
                    reported: None,
                    failures: attempt,
                    registration: None,
                },
            );
        }
        let all: Vec<u64> = self.tracked.keys().copied().collect();
        let reset: Vec<u64> = plan.registered.iter().map(|(id, _)| *id).collect();
        reset_sorter_regions(&mut self.sorter, &all, &reset);
        for region in fresh.iter().filter(|f| reset.contains(&f.id)) {
            self.assembler.forget_region_prewrites(&region.range);
            self.register(region.id, &region.store_address);
        }
    }

    fn register(&mut self, region_id: u64, store_address: &str) {
        let request_id = self.next_id();
        let (stream, requests) = self.stream_to(store_address);
        let region = self
            .tracked
            .get_mut(&region_id)
            .expect("registering a tracked region");
        let request = registration_request(self.cluster_id, region_id, region, request_id);
        debug!(
            region_id,
            request_id,
            checkpoint_ts = region.checkpoint,
            "registering cdc region"
        );
        if requests.send(request).is_err() {
            self.lose_registration(region_id);
            return;
        }
        region.registration = Some(Registration {
            request_id,
            stream,
            initialized: false,
        });
    }

    fn deregister(&self, region_id: u64) {
        let Some(registration) = self
            .tracked
            .get(&region_id)
            .and_then(|region| region.registration.as_ref())
        else {
            return;
        };
        let stream = self
            .streams
            .values()
            .find(|handle| handle.id == registration.stream);
        if let Some(stream) = stream {
            let request =
                deregistration_request(self.cluster_id, region_id, registration.request_id);
            let _ = stream.requests.send(request);
        }
    }

    fn stream_to(
        &mut self,
        store_address: &str,
    ) -> (u64, mpsc::UnboundedSender<ChangeDataRequest>) {
        if let Some(handle) = self.streams.get(store_address) {
            return (handle.id, handle.requests.clone());
        }
        let id = self.next_id();
        let (requests, requests_rx) = mpsc::unbounded_channel();
        let reader = tokio::spawn(read_stream(
            id,
            store_address.to_owned(),
            requests_rx,
            self.incoming.clone(),
        ));
        let handle = StreamHandle {
            id,
            requests: requests.clone(),
            reader,
        };
        self.streams.insert(store_address.to_owned(), handle);
        (id, requests)
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    fn start_lock_cleanup(&mut self) {
        if self.cleanup_running || self.last_cleanup_check.elapsed() < LOCK_CHECK_INTERVAL {
            return;
        }
        self.last_cleanup_check = Instant::now();
        let candidates: Vec<(KeyRange, u64)> = self
            .tracked
            .values()
            .filter(|region| region.registration.as_ref().is_some_and(|r| r.initialized))
            .map(|region| {
                (
                    region.range.clone(),
                    region.reported.unwrap_or(region.checkpoint),
                )
            })
            .collect();
        self.cleanup_running = true;
        let store = self.store.clone();
        let threshold = self.config.stalled_lock_threshold;
        let incoming = self.incoming.clone();
        tokio::spawn(async move {
            clean_stalled_locks(&store, candidates, threshold).await;
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

fn fresh_region(watched: &KeyRange, route: RegionRoute) -> Option<FreshRegion> {
    let range = clamp_to(watched, &route.region.start_key, &route.region.end_key)?;
    Some(FreshRegion {
        id: route.region.id,
        range,
        epoch: route.region.region_epoch.unwrap_or_default(),
        store_address: route.store_address,
    })
}

fn registration_request(
    cluster_id: u64,
    region_id: u64,
    region: &Region,
    request_id: u64,
) -> ChangeDataRequest {
    ChangeDataRequest {
        header: Some(header(cluster_id)),
        region_id,
        region_epoch: Some(region.epoch.clone()),
        checkpoint_ts: region.checkpoint,
        start_key: memcomparable_key(&region.range.0),
        end_key: memcomparable_key(&region.range.1),
        request_id,
        extra_op: ExtraOp::ReadOldValue as i32,
        kv_api: KvApi::TiDb as i32,
        filter_loop: false,
        request: Some(Request::Register(Register {})),
    }
}

fn deregistration_request(cluster_id: u64, region_id: u64, request_id: u64) -> ChangeDataRequest {
    ChangeDataRequest {
        header: Some(header(cluster_id)),
        region_id,
        request_id,
        request: Some(Request::Deregister(Deregister {})),
        ..Default::default()
    }
}

fn header(cluster_id: u64) -> Header {
    Header {
        cluster_id,
        ticdc_version: PER_REGION_RESOLVED_TS_VERSION.to_owned(),
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

async fn clean_stalled_locks(store: &Store, candidates: Vec<(KeyRange, u64)>, threshold: Duration) {
    let now = match store.now().await {
        Ok(now) => now as u64,
        Err(error) => {
            warn!(%error, "fetching a timestamp for lock cleanup failed");
            return;
        }
    };
    let safepoint = Timestamp::from_version(cleanup_safepoint(now, threshold));
    for (range, resolved) in candidates {
        if !lagging(resolved, now, threshold) {
            continue;
        }
        info!(
            resolved_ts = resolved,
            safepoint = safepoint.version(),
            "cleaning up stalled locks"
        );
        if let Err(error) = store
            .client()
            .cleanup_locks(bound(&range), &safepoint, ResolveLocksOptions::default())
            .await
        {
            warn!(%error, "cleaning up stalled locks failed");
        }
    }
}

async fn read_stream(
    stream: u64,
    store_address: String,
    requests: mpsc::UnboundedReceiver<ChangeDataRequest>,
    incoming: mpsc::Sender<Incoming>,
) {
    let error = match pump_stream(&store_address, requests, &incoming).await {
        Ok(()) => "stream ended".to_owned(),
        Err(error) => error.to_string(),
    };
    let _ = incoming.send(Incoming::Closed { stream, error }).await;
}

async fn pump_stream(
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
        if incoming.send(Incoming::Message(event)).await.is_err() {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Record;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02X}")).collect()
    }

    #[test]
    fn encodes_the_observed_data_range() {
        assert_eq!(hex(&memcomparable_key(b"k/")), "6B2F000000000000F9");
        assert_eq!(hex(&memcomparable_key(b"k0")), "6B30000000000000F9");
    }

    #[test]
    fn eight_byte_key_gets_a_trailing_empty_group() {
        assert_eq!(
            hex(&memcomparable_key(b"abcdefgh")),
            "6162636465666768FF0000000000000000F7"
        );
    }

    #[test]
    fn longer_key_spans_groups() {
        assert_eq!(
            hex(&memcomparable_key(b"abcdefghij")),
            "6162636465666768FF696A000000000000F9"
        );
    }

    #[test]
    fn empty_key_is_one_padded_group() {
        assert_eq!(hex(&memcomparable_key(b"")), "0000000000000000F7");
    }

    #[test]
    fn encoding_preserves_order() {
        let keys: [&[u8]; 6] = [b"", b"k", b"k/", b"k/\0", b"k/abcdefgh", b"k/abcdefgh\0"];
        for pair in keys.windows(2) {
            assert!(
                memcomparable_key(pair[0]) < memcomparable_key(pair[1]),
                "{pair:?}"
            );
        }
    }

    fn watched() -> KeyRange {
        (b"k/".to_vec(), b"k0".to_vec())
    }

    #[test]
    fn region_covering_everything_is_clamped_to_the_watched_range() {
        assert_eq!(clamp_to(&watched(), b"", b""), Some(watched()));
    }

    #[test]
    fn region_inside_the_watched_range_keeps_its_bounds() {
        assert_eq!(
            clamp_to(&watched(), b"k/a", b"k/m"),
            Some((b"k/a".to_vec(), b"k/m".to_vec()))
        );
    }

    #[test]
    fn region_straddling_an_edge_is_cut_at_it() {
        assert_eq!(
            clamp_to(&watched(), b"j", b"k/m"),
            Some((b"k/".to_vec(), b"k/m".to_vec()))
        );
        assert_eq!(
            clamp_to(&watched(), b"k/m", b""),
            Some((b"k/m".to_vec(), b"k0".to_vec()))
        );
    }

    #[test]
    fn region_outside_the_watched_range_is_none() {
        assert_eq!(clamp_to(&watched(), b"", b"k/"), None);
        assert_eq!(clamp_to(&watched(), b"k0", b""), None);
        assert_eq!(clamp_to(&watched(), b"x", b"y"), None);
    }

    fn epoch(version: u64) -> RegionEpoch {
        RegionEpoch {
            conf_ver: 1,
            version,
        }
    }

    fn range(start: &str, end: &str) -> KeyRange {
        (start.as_bytes().to_vec(), end.as_bytes().to_vec())
    }

    fn tracked(start: &str, end: &str, version: u64, checkpoint: u64, live: bool) -> Region {
        Region {
            range: range(start, end),
            epoch: epoch(version),
            checkpoint,
            reported: None,
            failures: 0,
            registration: live.then_some(Registration {
                request_id: 1,
                stream: 1,
                initialized: true,
            }),
        }
    }

    fn fresh(id: u64, start: &str, end: &str, version: u64) -> FreshRegion {
        FreshRegion {
            id,
            range: range(start, end),
            epoch: epoch(version),
            store_address: "tikv:20160".to_owned(),
        }
    }

    #[test]
    fn initial_lookup_registers_everything_at_the_start_timestamp() {
        let plan = plan_replacement(
            &BTreeMap::new(),
            &[fresh(1, "a", "m", 1), fresh(2, "m", "z", 1)],
            100,
        );
        assert_eq!(
            plan,
            Replacement {
                dropped: vec![],
                registered: vec![(1, 100), (2, 100)],
            }
        );
    }

    #[test]
    fn leader_change_re_registers_the_same_region_from_its_checkpoint() {
        let regions = BTreeMap::from([
            (1, tracked("a", "m", 1, 300, false)),
            (2, tracked("m", "z", 1, 500, true)),
        ]);
        let plan = plan_replacement(&regions, &[fresh(1, "a", "m", 1)], 0);
        assert_eq!(
            plan,
            Replacement {
                dropped: vec![],
                registered: vec![(1, 300)],
            }
        );
    }

    #[test]
    fn split_registers_both_halves_from_the_old_checkpoint() {
        let regions = BTreeMap::from([
            (1, tracked("a", "m", 1, 300, false)),
            (2, tracked("m", "z", 1, 500, true)),
        ]);
        let plan = plan_replacement(&regions, &[fresh(1, "a", "f", 2), fresh(3, "f", "m", 2)], 0);
        assert_eq!(
            plan,
            Replacement {
                dropped: vec![],
                registered: vec![(1, 300), (3, 300)],
            }
        );
    }

    #[test]
    fn split_where_the_old_id_goes_away_drops_it() {
        let regions = BTreeMap::from([(1, tracked("a", "m", 1, 300, false))]);
        let plan = plan_replacement(&regions, &[fresh(4, "a", "f", 2), fresh(3, "f", "m", 2)], 0);
        assert_eq!(
            plan,
            Replacement {
                dropped: vec![1],
                registered: vec![(4, 300), (3, 300)],
            }
        );
    }

    #[test]
    fn merge_inherits_the_lowest_checkpoint_of_what_it_covers() {
        let regions = BTreeMap::from([
            (1, tracked("a", "m", 1, 300, false)),
            (2, tracked("m", "z", 1, 500, true)),
        ]);
        let plan = plan_replacement(&regions, &[fresh(2, "a", "z", 2)], 0);
        assert_eq!(
            plan,
            Replacement {
                dropped: vec![1],
                registered: vec![(2, 300)],
            }
        );
    }

    #[test]
    fn a_live_region_with_the_same_epoch_is_left_alone() {
        let regions = BTreeMap::from([
            (1, tracked("a", "m", 1, 300, false)),
            (2, tracked("m", "z", 1, 500, true)),
        ]);
        let plan = plan_replacement(&regions, &[fresh(1, "a", "m", 1), fresh(2, "m", "z", 1)], 0);
        assert_eq!(
            plan,
            Replacement {
                dropped: vec![],
                registered: vec![(1, 300)],
            }
        );
    }

    #[test]
    fn an_old_region_only_partly_covered_is_kept() {
        let regions = BTreeMap::from([
            (1, tracked("a", "m", 1, 300, false)),
            (2, tracked("m", "z", 1, 500, true)),
        ]);
        let plan = plan_replacement(&regions, &[fresh(5, "a", "q", 3)], 0);
        assert_eq!(
            plan,
            Replacement {
                dropped: vec![1],
                registered: vec![(5, 300)],
            }
        );
    }

    #[test]
    fn a_fresh_region_overlapping_nothing_tracked_uses_the_fallback() {
        let regions = BTreeMap::from([(1, tracked("a", "m", 1, 300, false))]);
        let plan = plan_replacement(&regions, &[fresh(6, "m", "z", 1)], 42);
        assert_eq!(
            plan,
            Replacement {
                dropped: vec![],
                registered: vec![(6, 42)],
            }
        );
    }

    #[test]
    fn coverage_is_checked_across_adjacent_fresh_regions() {
        let fresh_regions = [fresh(1, "a", "f", 1), fresh(2, "f", "m", 1)];
        assert!(is_covered(&range("a", "m"), &fresh_regions));
        assert!(is_covered(&range("b", "g"), &fresh_regions));
        assert!(!is_covered(&range("a", "n"), &fresh_regions));
        assert!(!is_covered(
            &range("a", "m"),
            &[fresh(1, "a", "f", 1), fresh(2, "g", "m", 1)]
        ));
    }

    #[test]
    fn reset_regions_hold_the_watermark_until_they_report_again() {
        let mut sorter = Sorter::new(1 << 20);
        sorter.set_regions(&[1, 2]);
        sorter.set_resolved(1, 500);
        sorter.set_resolved(2, 300);
        assert_eq!(sorter.drain().1, 300);

        reset_sorter_regions(&mut sorter, &[1, 2], &[2]);
        sorter.set_resolved(1, 900);
        assert_eq!(sorter.drain().1, 300);

        sorter.set_resolved(2, 250);
        assert_eq!(sorter.drain().1, 300);
        sorter.set_resolved(2, 700);
        assert_eq!(sorter.drain().1, 700);
    }

    #[test]
    fn a_merged_region_cannot_keep_a_resolved_ts_above_what_it_inherited() {
        let mut sorter = Sorter::new(1 << 20);
        sorter.set_regions(&[1, 2]);
        sorter.set_resolved(1, 300);
        sorter.set_resolved(2, 500);
        assert_eq!(sorter.drain().1, 300);

        reset_sorter_regions(&mut sorter, &[2], &[2]);
        sorter
            .push(ChangeEvent {
                key: b"a".to_vec(),
                revision: 400,
                current: Some(Record {
                    version: 1,
                    created: 1,
                    lease_expiry: 0,
                    value: b"v".to_vec(),
                }),
                previous: None,
            })
            .unwrap();
        assert_eq!(sorter.drain(), (Vec::new(), 300));
    }

    fn ts(physical_ms: u64) -> u64 {
        physical_ms << PHYSICAL_SHIFT
    }

    #[test]
    fn a_region_is_lagging_only_past_the_threshold() {
        let threshold = Duration::from_secs(5);
        assert!(!lagging(ts(10_000), ts(15_000), threshold));
        assert!(lagging(ts(10_000), ts(15_001), threshold));
        assert!(!lagging(ts(20_000), ts(15_000), threshold));
    }

    #[test]
    fn cleanup_safepoint_is_the_threshold_before_now() {
        assert_eq!(
            cleanup_safepoint(ts(15_000) + 7, Duration::from_secs(5)),
            ts(10_000)
        );
        assert_eq!(cleanup_safepoint(ts(1_000), Duration::from_secs(5)), 0);
    }

    fn event(revision: i64) -> ChangeEvent {
        ChangeEvent {
            key: b"a".to_vec(),
            revision,
            current: None,
            previous: None,
        }
    }

    #[test]
    fn nothing_is_released_below_the_start_timestamp() {
        assert_eq!(release(vec![], 99, 100, None), None);
    }

    #[test]
    fn the_first_watermark_at_or_above_the_start_is_released() {
        assert_eq!(
            release(vec![], 100, 100, None),
            Some(FeedOutput::Events {
                events: vec![],
                watermark: 100
            })
        );
    }

    #[test]
    fn an_unmoved_watermark_with_no_events_is_not_released() {
        assert_eq!(release(vec![], 150, 100, Some(150)), None);
    }

    #[test]
    fn a_moved_watermark_or_events_are_released() {
        assert_eq!(
            release(vec![], 160, 100, Some(150)),
            Some(FeedOutput::Events {
                events: vec![],
                watermark: 160
            })
        );
        assert_eq!(
            release(vec![event(150)], 150, 100, Some(150)),
            Some(FeedOutput::Events {
                events: vec![event(150)],
                watermark: 150
            })
        );
    }

    #[test]
    fn retry_delay_grows_and_is_capped() {
        assert_eq!(retry_delay(0), FIRST_RETRY);
        assert_eq!(retry_delay(1), FIRST_RETRY * 2);
        assert!(retry_delay(3) > retry_delay(2));
        assert_eq!(retry_delay(40), LONGEST_RETRY);
    }
}
