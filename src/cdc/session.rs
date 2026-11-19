use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use tikv_client::RegionRoute;
use tikv_client::proto::cdcpb::change_data_request::{Deregister, KvApi, Register, Request};
use tikv_client::proto::cdcpb::event::{Event as RegionEventKind, LogType, Row};
use tikv_client::proto::cdcpb::{ChangeDataEvent, ChangeDataRequest, Event as RegionEvent, Header};
use tikv_client::proto::kvrpcpb::ExtraOp;
use tikv_client::proto::metapb::RegionEpoch;
use tracing::{debug, warn};

use super::encoding::memcomparable_key;
use super::plan::{Coverage, FreshRegion, KeyRange, fresh_region, is_covered, plan_replacement};
use super::{Assembler, ChangeEvent, Sorter};
use crate::keys::data_range;

const PHYSICAL_SHIFT: u32 = 18;
const PER_REGION_RESOLVED_TS_VERSION: &str = "";
const FIRST_RETRY: Duration = Duration::from_millis(50);
const LONGEST_RETRY: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq)]
pub enum FeedOutput {
    Events {
        events: Vec<ChangeEvent>,
        watermark: i64,
    },
    Reset,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    Register {
        stream: u64,
        store_address: String,
        request: ChangeDataRequest,
    },
    Deregister {
        stream: u64,
        request: ChangeDataRequest,
    },
    LookUp {
        range: KeyRange,
        attempt: u32,
        after: Duration,
    },
    CleanLocks {
        range: KeyRange,
        safepoint: u64,
    },
    Emit(FeedOutput),
    Reset,
}

#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub floor: u64,
    pub cluster_id: u64,
    pub sorter_memory_limit: usize,
    pub stalled_lock_threshold: Duration,
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

impl Region {
    fn coverage(&self) -> Coverage {
        Coverage {
            range: self.range.clone(),
            epoch: self.epoch.clone(),
            checkpoint: self.checkpoint,
            live: self.registration.is_some(),
        }
    }
}

struct SorterFullError;

pub struct Session {
    config: SessionConfig,
    sorter: Sorter,
    assembler: Assembler,
    tracked: BTreeMap<u64, Region>,
    streams: HashMap<String, u64>,
    next_id: u64,
    last_emitted: Option<i64>,
    actions: Vec<Action>,
}

pub fn watched_range() -> KeyRange {
    data_range(b"", b"\0")
}

impl Session {
    pub fn new(config: SessionConfig) -> (Session, Vec<Action>) {
        let sorter = Sorter::new(config.sorter_memory_limit);
        let session = Session {
            config,
            sorter,
            assembler: Assembler::new(),
            tracked: BTreeMap::new(),
            streams: HashMap::new(),
            next_id: 1,
            last_emitted: None,
            actions: Vec::new(),
        };
        let first_lookup = Action::LookUp {
            range: watched_range(),
            attempt: 0,
            after: Duration::ZERO,
        };
        (session, vec![first_lookup])
    }

    pub fn on_change_data(&mut self, stream: u64, event: ChangeDataEvent) -> Vec<Action> {
        if self.apply_change_data(stream, event).is_err() {
            warn!("cdc sorter is full, resetting the feed");
            return vec![Action::Reset];
        }
        self.take_actions()
    }

    pub fn on_stream_closed(&mut self, stream: u64) -> Vec<Action> {
        self.streams.retain(|_, id| *id != stream);
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
        self.take_actions()
    }

    pub fn on_lookup(
        &mut self,
        range: KeyRange,
        attempt: u32,
        routes: Result<Vec<RegionRoute>, String>,
    ) -> Vec<Action> {
        let routes = match routes {
            Ok(routes) => routes,
            Err(error) => {
                warn!(%error, attempt, "looking up cdc regions failed, retrying");
                self.look_up_later(range, attempt + 1);
                return self.take_actions();
            }
        };
        let fresh: Vec<FreshRegion> = routes
            .into_iter()
            .filter_map(|route| fresh_region(&watched_range(), route))
            .collect();
        if !is_covered(&range, &fresh) {
            warn!(attempt, "cdc region lookup left a gap, retrying");
            self.look_up_later(range, attempt + 1);
            return self.take_actions();
        }
        self.replace_regions(&fresh, attempt);
        self.take_actions()
    }

    pub fn on_tick(&mut self) -> Vec<Action> {
        let (events, watermark) = self.sorter.drain();
        if let Some(output) = release(events, watermark, self.config.floor, self.last_emitted) {
            self.last_emitted = Some(watermark);
            self.actions.push(Action::Emit(output));
        }
        self.take_actions()
    }

    pub fn on_clock(&mut self, now: u64) -> Vec<Action> {
        let threshold = self.config.stalled_lock_threshold;
        let safepoint = cleanup_safepoint(now, threshold);
        let stalled: Vec<KeyRange> = self
            .tracked
            .values()
            .filter(|region| region.registration.as_ref().is_some_and(|r| r.initialized))
            .filter(|region| lagging(region.reported.unwrap_or(region.checkpoint), now, threshold))
            .map(|region| region.range.clone())
            .collect();
        for range in stalled {
            self.actions.push(Action::CleanLocks { range, safepoint });
        }
        self.take_actions()
    }

    fn take_actions(&mut self) -> Vec<Action> {
        std::mem::take(&mut self.actions)
    }

    fn apply_change_data(
        &mut self,
        stream: u64,
        event: ChangeDataEvent,
    ) -> Result<(), SorterFullError> {
        for region_event in event.events {
            self.on_region_event(stream, region_event)?;
        }
        if let Some(resolved) = event.resolved_ts {
            for region_id in resolved.regions {
                self.on_resolved_ts(stream, region_id, resolved.request_id, resolved.ts);
            }
        }
        Ok(())
    }

    fn on_region_event(&mut self, stream: u64, event: RegionEvent) -> Result<(), SorterFullError> {
        let region_id = event.region_id;
        if self
            .registration(stream, region_id, event.request_id)
            .is_none()
        {
            return Ok(());
        }
        match event.event {
            Some(RegionEventKind::Entries(entries)) => {
                for row in entries.entries {
                    if self
                        .registration(stream, region_id, event.request_id)
                        .is_none()
                    {
                        break;
                    }
                    self.on_row(region_id, row)?;
                }
            }
            Some(RegionEventKind::Error(error)) => {
                warn!(region_id, ?error, "cdc region error, re-registering");
                self.lose_registration(region_id);
            }
            Some(RegionEventKind::ResolvedTs(ts)) => {
                self.on_resolved_ts(stream, region_id, event.request_id, ts)
            }
            _ => {}
        }
        Ok(())
    }

    fn on_row(&mut self, region_id: u64, row: Row) -> Result<(), SorterFullError> {
        if row.r#type == LogType::Initialized as i32 {
            if let Some(region) = self.tracked.get_mut(&region_id) {
                region.failures = 0;
                if let Some(registration) = region.registration.as_mut() {
                    registration.initialized = true;
                }
            }
            return Ok(());
        }
        self.assemble(region_id, row)
    }

    fn assemble(&mut self, region_id: u64, row: Row) -> Result<(), SorterFullError> {
        match self.assembler.on_row(row) {
            Ok(Some(event)) if event.revision as u64 > self.config.floor => {
                self.sorter.push(event).map_err(|_| SorterFullError)
            }
            Ok(_) => Ok(()),
            Err(_) => {
                warn!(region_id, "skipping a cdc row whose record does not decode");
                Ok(())
            }
        }
    }

    fn on_resolved_ts(&mut self, stream: u64, region_id: u64, request_id: u64, ts: u64) {
        let initialized = self
            .registration(stream, region_id, request_id)
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

    fn registration(&self, stream: u64, region_id: u64, request_id: u64) -> Option<&Registration> {
        self.tracked
            .get(&region_id)?
            .registration
            .as_ref()
            .filter(|registration| {
                registration.request_id == request_id && registration.stream == stream
            })
    }

    fn lose_registration(&mut self, region_id: u64) {
        self.deregister(region_id);
        let Some(region) = self.tracked.get_mut(&region_id) else {
            return;
        };
        region.registration = None;
        region.failures += 1;
        let (range, attempt) = (region.range.clone(), region.failures);
        self.look_up_later(range, attempt);
    }

    fn look_up_later(&mut self, range: KeyRange, attempt: u32) {
        self.actions.push(Action::LookUp {
            range,
            attempt,
            after: retry_delay(attempt),
        });
    }

    fn replace_regions(&mut self, fresh: &[FreshRegion], attempt: u32) {
        let fallback = self
            .tracked
            .values()
            .map(|region| region.checkpoint)
            .min()
            .unwrap_or(self.config.floor);
        let coverage: BTreeMap<u64, Coverage> = self
            .tracked
            .iter()
            .map(|(id, region)| (*id, region.coverage()))
            .collect();
        let plan = plan_replacement(&coverage, fresh, fallback);
        let superseded: Vec<u64> = plan
            .dropped
            .iter()
            .copied()
            .chain(plan.registered.iter().map(|(id, _)| *id))
            .collect();
        for region_id in superseded {
            self.deregister(region_id);
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
        let stream = self.stream_to(store_address);
        let cluster_id = self.config.cluster_id;
        let region = self
            .tracked
            .get_mut(&region_id)
            .expect("registering a tracked region");
        let request = registration_request(cluster_id, region_id, region, request_id);
        debug!(
            region_id,
            request_id,
            checkpoint_ts = request.checkpoint_ts,
            "registering cdc region"
        );
        region.registration = Some(Registration {
            request_id,
            stream,
            initialized: false,
        });
        self.actions.push(Action::Register {
            stream,
            store_address: store_address.to_owned(),
            request,
        });
    }

    fn deregister(&mut self, region_id: u64) {
        let Some(registration) = self
            .tracked
            .get(&region_id)
            .and_then(|region| region.registration.as_ref())
        else {
            return;
        };
        let request =
            deregistration_request(self.config.cluster_id, region_id, registration.request_id);
        self.actions.push(Action::Deregister {
            stream: registration.stream,
            request,
        });
    }

    fn stream_to(&mut self, store_address: &str) -> u64 {
        if let Some(stream) = self.streams.get(store_address) {
            return *stream;
        }
        let stream = self.next_id();
        self.streams.insert(store_address.to_owned(), stream);
        stream
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
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

pub fn retry_delay(attempt: u32) -> Duration {
    FIRST_RETRY
        .saturating_mul(1 << attempt.min(16))
        .min(LONGEST_RETRY)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Record;

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
