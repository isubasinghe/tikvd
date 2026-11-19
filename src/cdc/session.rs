use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use tikv_client::RegionRoute;
use tikv_client::proto::cdcpb::change_data_request::{Deregister, KvApi, Register, Request};
use tikv_client::proto::cdcpb::event::{Event as RegionEventKind, LogType, Row};
use tikv_client::proto::cdcpb::{ChangeDataEvent, ChangeDataRequest, Event as RegionEvent, Header};
use tikv_client::proto::kvrpcpb::ExtraOp;
use tikv_client::proto::metapb::RegionEpoch;
use tracing::{debug, warn};

use super::encoding::memcomparable_key;
use super::plan::{
    Coverage, FreshRegion, KeyRange, Replacement, fresh_region, is_covered, is_older_than_tracked,
    plan_replacement,
};
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
pub struct LookedUp {
    pub routes: Vec<RegionRoute>,
    pub compact_revision: u64,
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
    // ponytail: bounded only by how many commits one region's incremental scan overlaps; cap it and re-register past the cap if scans get long
    early_commits: Vec<Row>,
    scanned_commits: HashSet<(Vec<u8>, u64)>,
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
        looked_up: Result<LookedUp, String>,
    ) -> Vec<Action> {
        let LookedUp {
            routes,
            compact_revision,
        } = match looked_up {
            Ok(looked_up) => looked_up,
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
        if is_older_than_tracked(&self.coverage(), &fresh) {
            warn!(
                attempt,
                "cdc region lookup is older than what is tracked, retrying"
            );
            self.look_up_later(range, attempt + 1);
            return self.take_actions();
        }
        self.replace_regions(&fresh, attempt, compact_revision);
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
            return self.on_initialized(region_id);
        }
        if self.assembler.is_unmatched_commit(&row) {
            self.on_unmatched_commit(region_id, row);
            return Ok(());
        }
        if row.r#type == LogType::Committed as i32 {
            self.note_scanned_commit(region_id, &row);
        }
        self.assemble(region_id, row)
    }

    fn on_initialized(&mut self, region_id: u64) -> Result<(), SorterFullError> {
        let region = self.current_region(region_id);
        let checkpoint = region.checkpoint;
        let registration = region.registration.as_mut().expect("current registration");
        registration.initialized = true;
        let early_commits = std::mem::take(&mut registration.early_commits);
        let scanned_commits = std::mem::take(&mut registration.scanned_commits);
        for row in early_commits {
            if !self.assembler.is_unmatched_commit(&row) {
                self.assemble(region_id, row)?;
                continue;
            }
            let seen_committed = scanned_commits.contains(&(row.key.clone(), row.start_ts));
            if row.commit_ts > checkpoint && !seen_committed {
                warn!(
                    region_id,
                    "a commit seen during the scan has no prewrite, re-registering"
                );
                self.lose_registration(region_id);
                return Ok(());
            }
        }
        Ok(())
    }

    fn on_unmatched_commit(&mut self, region_id: u64, row: Row) {
        let region = self.current_region(region_id);
        if row.commit_ts <= region.checkpoint {
            return;
        }
        let registration = region.registration.as_mut().expect("current registration");
        if !registration.initialized {
            registration.early_commits.push(row);
            return;
        }
        warn!(
            region_id,
            "a commit arrived with no prewrite, re-registering"
        );
        self.lose_registration(region_id);
    }

    fn note_scanned_commit(&mut self, region_id: u64, row: &Row) {
        let registration = self
            .current_region(region_id)
            .registration
            .as_mut()
            .expect("current registration");
        if !registration.initialized {
            registration
                .scanned_commits
                .insert((row.key.clone(), row.start_ts));
        }
    }

    fn current_region(&mut self, region_id: u64) -> &mut Region {
        self.tracked
            .get_mut(&region_id)
            .expect("rows come only from a tracked region's current registration")
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
        region.failures = 0;
        self.assembler.on_resolved_ts(&region.range, request_id, ts);
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

    fn replace_regions(&mut self, fresh: &[FreshRegion], attempt: u32, compact_revision: u64) {
        let fallback = self
            .tracked
            .values()
            .map(|region| region.checkpoint)
            .min()
            .unwrap_or(self.config.floor);
        let plan = plan_replacement(&self.coverage(), fresh, fallback);
        if self.inherits_compacted_checkpoint(&plan, compact_revision) {
            warn!(
                compact_revision,
                "a cdc region would resume below the compaction revision, resetting the feed"
            );
            self.actions.push(Action::Reset);
            return;
        }
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
            self.register(region.id, &region.store_address);
        }
    }

    fn inherits_compacted_checkpoint(&self, plan: &Replacement, compact_revision: u64) -> bool {
        let lowest = plan
            .registered
            .iter()
            .map(|(_, checkpoint)| *checkpoint)
            .min();
        // ponytail: a fresh session skips this check, safe only because the hub vouches from the first watermark; check the floor too if the hub ever vouches earlier
        !self.tracked.is_empty() && lowest.is_some_and(|lowest| lowest < compact_revision)
    }

    fn coverage(&self) -> BTreeMap<u64, Coverage> {
        self.tracked
            .iter()
            .map(|(id, region)| (*id, region.coverage()))
            .collect()
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
            early_commits: Vec::new(),
            scanned_commits: HashSet::new(),
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
        checkpoint_ts: region.checkpoint.saturating_sub(1),
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

    use crate::keys::data_key;
    use tikv_client::proto::cdcpb::event::Entries;
    use tikv_client::proto::cdcpb::event::row::OpType;
    use tikv_client::proto::cdcpb::{Error as CdcError, ResolvedTs};
    use tikv_client::proto::metapb::Region as MetaRegion;

    const STORE: &str = "tikv:20160";

    #[derive(Debug, Clone, PartialEq)]
    struct Reg {
        region_id: u64,
        request_id: u64,
        stream: u64,
        checkpoint: u64,
    }

    fn session(floor: u64) -> Session {
        Session::new(SessionConfig {
            floor,
            cluster_id: 1,
            sorter_memory_limit: 1 << 20,
            stalled_lock_threshold: Duration::from_secs(5),
        })
        .0
    }

    fn raw(key: &str) -> Vec<u8> {
        if key.is_empty() {
            Vec::new()
        } else {
            data_key(key.as_bytes())
        }
    }

    fn route(id: u64, start: &str, end: &str, version: u64) -> RegionRoute {
        RegionRoute {
            region: MetaRegion {
                id,
                start_key: raw(start),
                end_key: if end == "~" { b"k0".to_vec() } else { raw(end) },
                region_epoch: Some(RegionEpoch {
                    conf_ver: 1,
                    version,
                }),
                ..Default::default()
            },
            leader: Default::default(),
            store_address: STORE.to_owned(),
        }
    }

    fn registrations(actions: &[Action]) -> Vec<Reg> {
        actions
            .iter()
            .filter_map(|action| match action {
                Action::Register {
                    stream, request, ..
                } => Some(Reg {
                    region_id: request.region_id,
                    request_id: request.request_id,
                    stream: *stream,
                    checkpoint: request.checkpoint_ts,
                }),
                _ => None,
            })
            .collect()
    }

    fn looked_up(actions: &[Action]) -> Vec<KeyRange> {
        actions
            .iter()
            .filter_map(|action| match action {
                Action::LookUp { range, .. } => Some(range.clone()),
                _ => None,
            })
            .collect()
    }

    fn deregistered(actions: &[Action]) -> Vec<u64> {
        actions
            .iter()
            .filter_map(|action| match action {
                Action::Deregister { request, .. } => Some(request.region_id),
                _ => None,
            })
            .collect()
    }

    fn emitted(actions: &[Action]) -> Vec<(Vec<ChangeEvent>, i64)> {
        actions
            .iter()
            .filter_map(|action| match action {
                Action::Emit(FeedOutput::Events { events, watermark }) => {
                    Some((events.clone(), *watermark))
                }
                _ => None,
            })
            .collect()
    }

    fn single(reg: &Reg, event: RegionEventKind) -> ChangeDataEvent {
        ChangeDataEvent {
            events: vec![RegionEvent {
                region_id: reg.region_id,
                request_id: reg.request_id,
                event: Some(event),
                ..Default::default()
            }],
            resolved_ts: None,
        }
    }

    fn rows(reg: &Reg, rows: Vec<Row>) -> ChangeDataEvent {
        single(reg, RegionEventKind::Entries(Entries { entries: rows }))
    }

    fn resolved(reg: &Reg, ts: u64) -> ChangeDataEvent {
        single(reg, RegionEventKind::ResolvedTs(ts))
    }

    fn batch_resolved(reg: &Reg, ts: u64) -> ChangeDataEvent {
        ChangeDataEvent {
            events: vec![],
            resolved_ts: Some(ResolvedTs {
                regions: vec![reg.region_id],
                ts,
                request_id: reg.request_id,
            }),
        }
    }

    fn region_error(reg: &Reg) -> ChangeDataEvent {
        single(reg, RegionEventKind::Error(CdcError::default()))
    }

    fn value(text: &str) -> Vec<u8> {
        Record {
            version: 1,
            created: 1,
            lease_expiry: 0,
            value: text.as_bytes().to_vec(),
        }
        .encode()
    }

    fn row(log_type: LogType, key: &str, start_ts: u64, commit_ts: u64) -> Row {
        Row {
            r#type: log_type as i32,
            op_type: OpType::Put as i32,
            key: raw(key),
            start_ts,
            commit_ts,
            ..Default::default()
        }
    }

    fn prewrite(key: &str, start_ts: u64) -> Row {
        Row {
            value: value("v"),
            ..row(LogType::Prewrite, key, start_ts, 0)
        }
    }

    fn commit(key: &str, start_ts: u64, commit_ts: u64) -> Row {
        row(LogType::Commit, key, start_ts, commit_ts)
    }

    fn committed(key: &str, start_ts: u64, commit_ts: u64) -> Row {
        Row {
            value: value("v"),
            ..row(LogType::Committed, key, start_ts, commit_ts)
        }
    }

    fn initialized() -> Row {
        row(LogType::Initialized, "", 0, 0)
    }

    fn keyed(events: &[(Vec<ChangeEvent>, i64)]) -> Vec<(Vec<u8>, i64)> {
        events
            .iter()
            .flat_map(|(events, _)| events.iter().map(|e| (e.key.clone(), e.revision)))
            .collect()
    }

    fn watermarks(events: &[(Vec<ChangeEvent>, i64)]) -> Vec<i64> {
        events.iter().map(|(_, watermark)| *watermark).collect()
    }

    fn found(routes: Vec<RegionRoute>) -> Result<LookedUp, String> {
        found_compacted_at(routes, 0)
    }

    fn found_compacted_at(
        routes: Vec<RegionRoute>,
        compact_revision: u64,
    ) -> Result<LookedUp, String> {
        Ok(LookedUp {
            routes,
            compact_revision,
        })
    }

    fn resets(actions: &[Action]) -> bool {
        actions.contains(&Action::Reset)
    }

    fn reregister_whole_after_error(session: &mut Session, compact_revision: u64) -> Vec<Action> {
        let reg = register_one(session, route(1, "", "~", 1));
        session.on_change_data(reg.stream, rows(&reg, vec![initialized()]));
        session.on_change_data(reg.stream, region_error(&reg));
        session.on_lookup(
            clamp_raw(&route(1, "", "~", 1)),
            1,
            found_compacted_at(vec![route(1, "", "~", 1)], compact_revision),
        )
    }

    #[test]
    fn reregistration_below_the_compaction_revision_resets() {
        let mut session = session(100);
        let actions = reregister_whole_after_error(&mut session, 150);
        assert!(resets(&actions), "{actions:?}");
        assert!(registrations(&actions).is_empty(), "{actions:?}");
    }

    #[test]
    fn reregistration_at_or_above_the_compaction_revision_proceeds() {
        let mut session = session(100);
        let actions = reregister_whole_after_error(&mut session, 100);
        assert!(!resets(&actions), "{actions:?}");
        assert_eq!(registrations(&actions).len(), 1, "{actions:?}");
        assert_eq!(registrations(&actions)[0].checkpoint, 99);
    }

    #[test]
    fn fresh_session_start_is_not_subject_to_the_compaction_check() {
        let mut session = session(100);
        let whole = route(1, "", "~", 1);
        let actions = session.on_lookup(clamp_raw(&whole), 0, found_compacted_at(vec![whole], 150));
        assert!(!resets(&actions), "{actions:?}");
        assert_eq!(registrations(&actions).len(), 1, "{actions:?}");
    }

    fn register_one(session: &mut Session, route: RegionRoute) -> Reg {
        let range = clamp_raw(&route);
        let actions = session.on_lookup(range, 0, found(vec![route]));
        registrations(&actions).remove(0)
    }

    fn clamp_raw(route: &RegionRoute) -> KeyRange {
        fresh_region(&watched_range(), route.clone()).unwrap().range
    }

    fn live(session: &mut Session, reg: &Reg, ts: u64) {
        session.on_change_data(reg.stream, rows(reg, vec![initialized()]));
        session.on_change_data(reg.stream, resolved(reg, ts));
    }

    #[test]
    fn commit_before_prewrite_during_scan_is_not_lost() {
        let mut session = session(100);
        let reg = register_one(&mut session, route(1, "", "~", 1));

        session.on_change_data(reg.stream, rows(&reg, vec![commit("x", 150, 200)]));
        let mut out = emitted(&session.on_tick());
        session.on_change_data(reg.stream, rows(&reg, vec![prewrite("x", 150)]));
        out.extend(emitted(&session.on_tick()));
        session.on_change_data(reg.stream, rows(&reg, vec![initialized()]));
        out.extend(emitted(&session.on_tick()));
        session.on_change_data(reg.stream, resolved(&reg, 300));
        out.extend(emitted(&session.on_tick()));

        assert_eq!(keyed(&out), vec![(b"x".to_vec(), 200)]);
        let first_past_commit = out.iter().position(|(_, w)| *w >= 200).unwrap();
        assert!(!out[first_past_commit].0.is_empty(), "{out:?}");
    }

    #[test]
    fn unmatched_commit_after_initialized_triggers_reregistration() {
        let mut session = session(100);
        let reg = register_one(&mut session, route(1, "", "~", 1));
        live(&mut session, &reg, 150);

        let actions = session.on_change_data(reg.stream, rows(&reg, vec![commit("y", 160, 200)]));
        assert_eq!(deregistered(&actions), vec![1]);
        assert_eq!(looked_up(&actions), vec![clamp_raw(&route(1, "", "~", 1))]);

        let again = register_one(&mut session, route(1, "", "~", 1));
        assert_eq!(again.checkpoint, 149);
        session.on_change_data(again.stream, rows(&again, vec![committed("y", 160, 200)]));
        live(&mut session, &again, 300);
        assert_eq!(
            keyed(&emitted(&session.on_tick())),
            vec![(b"y".to_vec(), 200)]
        );
    }

    #[test]
    fn stale_resolved_ts_from_previous_registration_is_ignored() {
        let mut session = session(100);
        let old = register_one(&mut session, route(1, "", "~", 1));
        live(&mut session, &old, 150);
        session.on_tick();
        session.on_change_data(old.stream, region_error(&old));
        let new = register_one(&mut session, route(1, "", "~", 1));

        session.on_change_data(old.stream, resolved(&old, 900));
        session.on_change_data(old.stream, batch_resolved(&old, 900));
        assert!(emitted(&session.on_tick()).is_empty());

        live(&mut session, &new, 200);
        assert_eq!(watermarks(&emitted(&session.on_tick())), vec![200]);
    }

    #[test]
    fn resolved_ts_before_initialized_is_ignored() {
        let mut session = session(100);
        let reg = register_one(&mut session, route(1, "", "~", 1));
        session.on_change_data(reg.stream, resolved(&reg, 500));
        session.on_change_data(reg.stream, batch_resolved(&reg, 500));
        assert!(emitted(&session.on_tick()).is_empty());

        live(&mut session, &reg, 200);
        assert_eq!(watermarks(&emitted(&session.on_tick())), vec![200]);
    }

    fn two_regions(session: &mut Session) -> (Reg, Reg) {
        two_regions_at(session, 150)
    }

    fn two_regions_at(session: &mut Session, resolved_ts: u64) -> (Reg, Reg) {
        let left = route(5, "", "m", 3);
        let right = route(2, "m", "~", 3);
        let actions = session.on_lookup(watched_range(), 0, found(vec![left, right]));
        let regs = registrations(&actions);
        let five = regs.iter().find(|r| r.region_id == 5).unwrap().clone();
        let two = regs.iter().find(|r| r.region_id == 2).unwrap().clone();
        live(session, &five, resolved_ts);
        live(session, &two, resolved_ts);
        session.on_tick();
        (five, two)
    }

    #[test]
    fn stale_lookup_result_is_discarded() {
        let mut session = session(100);
        let (five, two) = two_regions(&mut session);
        session.on_change_data(five.stream, region_error(&five));

        let stale = route(5, "", "q", 2);
        let actions = session.on_lookup(clamp_raw(&route(5, "", "m", 3)), 1, found(vec![stale]));
        assert!(registrations(&actions).is_empty(), "{actions:?}");
        assert!(deregistered(&actions).is_empty(), "{actions:?}");
        assert_eq!(looked_up(&actions).len(), 1);

        session.on_change_data(two.stream, resolved(&two, 400));
        assert!(emitted(&session.on_tick()).is_empty());
    }

    #[test]
    fn kept_registration_keeps_its_prewrites_across_a_neighbours_reregistration() {
        let mut session = session(100);
        let (five, two) = two_regions(&mut session);
        session.on_change_data(two.stream, rows(&two, vec![prewrite("n", 160)]));

        session.on_change_data(five.stream, region_error(&five));
        let grown = register_one(&mut session, route(5, "", "q", 4));
        session.on_change_data(two.stream, rows(&two, vec![commit("n", 160, 200)]));
        session.on_change_data(two.stream, resolved(&two, 300));
        live(&mut session, &grown, 300);

        let out = emitted(&session.on_tick());
        assert_eq!(keyed(&out), vec![(b"n".to_vec(), 200)]);
    }

    #[test]
    fn split_never_lets_the_watermark_pass_the_uncovered_half() {
        let mut session = session(100);
        let whole = register_one(&mut session, route(1, "", "~", 1));
        live(&mut session, &whole, 150);
        assert_eq!(watermarks(&emitted(&session.on_tick())), vec![150]);

        session.on_change_data(whole.stream, region_error(&whole));
        let actions = session.on_lookup(
            clamp_raw(&route(1, "", "~", 1)),
            1,
            found(vec![route(1, "", "m", 2), route(3, "m", "~", 2)]),
        );
        let regs = registrations(&actions);
        let left = regs.iter().find(|r| r.region_id == 1).unwrap().clone();
        let right = regs.iter().find(|r| r.region_id == 3).unwrap().clone();

        live(&mut session, &left, 500);
        assert!(emitted(&session.on_tick()).is_empty());

        session.on_change_data(right.stream, rows(&right, vec![committed("z", 290, 300)]));
        live(&mut session, &right, 500);
        let out = emitted(&session.on_tick());
        assert_eq!(keyed(&out), vec![(b"z".to_vec(), 300)]);
        assert_eq!(watermarks(&out), vec![500]);
    }

    #[test]
    fn stream_close_holds_the_watermark_until_reregistered() {
        let mut session = session(100);
        let reg = register_one(&mut session, route(1, "", "~", 1));
        live(&mut session, &reg, 150);
        session.on_tick();

        let actions = session.on_stream_closed(reg.stream);
        assert_eq!(looked_up(&actions), vec![clamp_raw(&route(1, "", "~", 1))]);
        session.on_change_data(reg.stream, resolved(&reg, 900));
        assert!(emitted(&session.on_tick()).is_empty());

        let again = register_one(&mut session, route(1, "", "~", 1));
        assert_ne!(again.stream, reg.stream);
        assert_eq!(again.checkpoint, 149);
        live(&mut session, &again, 500);
        assert_eq!(watermarks(&emitted(&session.on_tick())), vec![500]);
    }

    fn reregistered(actions: &[Action]) -> bool {
        actions
            .iter()
            .any(|a| matches!(a, Action::Register { .. } | Action::Deregister { .. }))
    }

    fn holds(session: &Session, key: &str, start_ts: u64) -> bool {
        !session
            .assembler
            .is_unmatched_commit(&commit(key, start_ts, start_ts + 1))
    }

    #[test]
    fn slow_transaction_prewriting_below_the_watermark_is_delivered_without_reregistration() {
        let mut session = session(10);
        let (five, two) = two_regions_at(&mut session, 100);
        let mut actions = Vec::new();

        actions.extend(session.on_change_data(five.stream, rows(&five, vec![prewrite("a", 50)])));
        for ts in [110, 120, 130] {
            actions.extend(session.on_change_data(five.stream, resolved(&five, 100)));
            actions.extend(session.on_change_data(two.stream, resolved(&two, ts)));
            actions.extend(session.on_tick());
        }
        actions
            .extend(session.on_change_data(five.stream, rows(&five, vec![commit("a", 50, 150)])));
        actions.extend(session.on_change_data(five.stream, resolved(&five, 200)));
        actions.extend(session.on_change_data(two.stream, resolved(&two, 200)));
        actions.extend(session.on_tick());

        assert_eq!(keyed(&emitted(&actions)), vec![(b"a".to_vec(), 150)]);
        assert!(!reregistered(&actions), "{actions:?}");
    }

    #[test]
    fn slow_prewrite_survives_repeated_unchanged_resolved_ts() {
        let mut session = session(10);
        let reg = register_one(&mut session, route(1, "", "~", 1));
        live(&mut session, &reg, 100);
        session.on_tick();
        let mut actions = Vec::new();

        actions.extend(session.on_change_data(reg.stream, rows(&reg, vec![prewrite("a", 50)])));
        for _ in 0..10 {
            actions.extend(session.on_change_data(reg.stream, resolved(&reg, 100)));
            actions.extend(session.on_tick());
        }
        actions.extend(session.on_change_data(reg.stream, rows(&reg, vec![commit("a", 50, 150)])));
        actions.extend(session.on_change_data(reg.stream, resolved(&reg, 200)));
        actions.extend(session.on_tick());

        assert_eq!(keyed(&emitted(&actions)), vec![(b"a".to_vec(), 150)]);
        assert!(!reregistered(&actions), "{actions:?}");
    }

    #[test]
    fn prewrite_is_dropped_after_two_resolved_timestamps_the_second_above_both() {
        let mut session = session(10);
        let reg = register_one(&mut session, route(1, "", "~", 1));
        live(&mut session, &reg, 40);
        session.on_change_data(reg.stream, rows(&reg, vec![prewrite("a", 50)]));
        session.on_change_data(reg.stream, resolved(&reg, 60));
        session.on_change_data(reg.stream, resolved(&reg, 70));
        assert!(!holds(&session, "a", 50));
    }

    #[test]
    fn prewrite_is_kept_after_a_single_higher_resolved_timestamp() {
        let mut session = session(10);
        let reg = register_one(&mut session, route(1, "", "~", 1));
        live(&mut session, &reg, 40);
        session.on_change_data(reg.stream, rows(&reg, vec![prewrite("a", 50)]));
        session.on_change_data(reg.stream, resolved(&reg, 60));
        session.on_tick();
        assert!(holds(&session, "a", 50));
    }

    #[test]
    fn prewrite_survives_a_resolved_timestamp_that_arrived_before_it() {
        let mut session = session(10);
        let (five, two) = two_regions_at(&mut session, 200);
        session.on_change_data(five.stream, rows(&five, vec![prewrite("a", 50)]));
        for ts in [250, 300] {
            session.on_change_data(five.stream, resolved(&five, 200));
            session.on_change_data(two.stream, resolved(&two, ts));
            session.on_tick();
        }
        assert!(holds(&session, "a", 50));

        session.on_change_data(five.stream, rows(&five, vec![commit("a", 50, 250)]));
        session.on_change_data(five.stream, resolved(&five, 300));
        assert_eq!(
            keyed(&emitted(&session.on_tick())),
            vec![(b"a".to_vec(), 250)]
        );
    }

    #[test]
    fn prewrite_survives_its_regions_reregistration_until_the_new_registration_resolves_past_it() {
        let mut session = session(10);
        let old = register_one(&mut session, route(1, "", "~", 1));
        live(&mut session, &old, 100);
        session.on_change_data(old.stream, rows(&old, vec![prewrite("a", 150)]));
        session.on_change_data(old.stream, resolved(&old, 160));
        session.on_change_data(old.stream, region_error(&old));

        let new = register_one(&mut session, route(1, "", "~", 1));
        session.on_change_data(new.stream, rows(&new, vec![initialized()]));
        session.on_change_data(new.stream, resolved(&new, 170));
        assert!(holds(&session, "a", 150));
        session.on_change_data(new.stream, resolved(&new, 180));
        assert!(!holds(&session, "a", 150));
    }

    #[test]
    fn a_commit_the_scan_already_delivered_does_not_reregister() {
        let mut session = session(100);
        let reg = register_one(&mut session, route(1, "", "~", 1));

        session.on_change_data(reg.stream, rows(&reg, vec![commit("c", 150, 200)]));
        session.on_change_data(reg.stream, rows(&reg, vec![committed("c", 150, 200)]));
        let actions = session.on_change_data(reg.stream, rows(&reg, vec![initialized()]));
        assert!(deregistered(&actions).is_empty(), "{actions:?}");

        session.on_change_data(reg.stream, resolved(&reg, 300));
        assert_eq!(
            keyed(&emitted(&session.on_tick())),
            vec![(b"c".to_vec(), 200)]
        );
    }
}
