use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use tokio::sync::{Notify, watch};

use crate::cdc::ChangeEvent;
use crate::cdc::feed::FeedOutput;

const BATCH_EVENTS: usize = 1000;

#[derive(Debug, PartialEq)]
pub struct Compacted;

#[derive(Debug, Clone, PartialEq)]
pub enum Delivery {
    Events {
        events: Vec<ChangeEvent>,
        watermark: i64,
    },
    Progress {
        watermark: i64,
    },
    Cancelled {
        compacted: bool,
    },
}

pub struct WatchHub {
    state: Mutex<HubState>,
    ready: watch::Sender<bool>,
    applied: watch::Sender<Applied>,
    limits: Limits,
}

#[derive(Clone, Copy, Default)]
struct Applied {
    watermark: i64,
    resets: u64,
}

#[derive(Clone, Copy)]
struct Limits {
    window_events: usize,
    window_bytes: usize,
    subscriber_buffer: usize,
    subscriber_bytes: usize,
    batch_events: usize,
}

#[derive(Default)]
struct HubState {
    watermark: i64,
    floor: Option<i64>,
    window: VecDeque<ChangeEvent>,
    window_bytes: usize,
    subscribers: Vec<Subscriber>,
}

struct Subscriber {
    key: Vec<u8>,
    range_end: Vec<u8>,
    after_revision: i64,
    queue: Arc<Queue>,
}

struct Queue {
    state: Mutex<QueueState>,
    notify: Notify,
}

struct QueueState {
    batches: VecDeque<Batch>,
    bytes: usize,
    watermark: i64,
    cancelled: Option<bool>,
}

struct Batch {
    events: Vec<ChangeEvent>,
    watermark: i64,
}

impl Batch {
    fn size_bytes(&self) -> usize {
        self.events.iter().map(ChangeEvent::size_bytes).sum()
    }
}

pub struct Subscription {
    queue: Arc<Queue>,
    reported: i64,
}

impl WatchHub {
    pub fn new(
        window_events: usize,
        window_bytes: usize,
        subscriber_buffer: usize,
        subscriber_bytes: usize,
    ) -> Arc<WatchHub> {
        Self::with_batch_limit(
            window_events,
            window_bytes,
            subscriber_buffer,
            subscriber_bytes,
            BATCH_EVENTS,
        )
    }

    fn with_batch_limit(
        window_events: usize,
        window_bytes: usize,
        subscriber_buffer: usize,
        subscriber_bytes: usize,
        batch_events: usize,
    ) -> Arc<WatchHub> {
        Arc::new(WatchHub {
            state: Mutex::new(HubState::default()),
            ready: watch::Sender::new(false),
            applied: watch::Sender::new(Applied::default()),
            limits: Limits {
                window_events,
                window_bytes,
                subscriber_buffer,
                subscriber_bytes,
                batch_events,
            },
        })
    }

    pub fn apply(&self, output: FeedOutput) {
        match output {
            FeedOutput::Events { events, watermark } => self.advance(events, watermark),
            FeedOutput::Reset => self.reset(),
        }
    }

    pub fn watermark(&self) -> i64 {
        self.state.lock().unwrap().watermark
    }

    pub fn oldest_revision(&self) -> i64 {
        let state = self.state.lock().unwrap();
        state.floor.unwrap_or(state.watermark) + 1
    }

    pub async fn wait_for_watermark(&self) {
        let _ = self.ready.subscribe().wait_for(|ready| *ready).await;
    }

    pub async fn delivered(&self, revision: i64) {
        let mut applied = self.applied.subscribe();
        let resets = applied.borrow().resets;
        let _ = applied
            .wait_for(|applied| applied.watermark >= revision || applied.resets != resets)
            .await;
    }

    pub fn subscribe(
        &self,
        key: Vec<u8>,
        range_end: Vec<u8>,
        after_revision: i64,
    ) -> Result<Subscription, Compacted> {
        let mut state = self.state.lock().unwrap();
        match state.floor {
            Some(floor) if after_revision >= floor => {}
            _ => return Err(Compacted),
        }
        let queue = Arc::new(Queue::new(state.watermark));
        let subscriber = Subscriber {
            key,
            range_end,
            after_revision,
            queue: queue.clone(),
        };
        let first_unseen = state
            .window
            .partition_point(|event| event.revision <= after_revision);
        let replay = state.window.range(first_unseen..);
        if !subscriber.offer(replay, state.watermark, self.limits) {
            return Err(Compacted);
        }
        state.subscribers.push(subscriber);
        Ok(Subscription {
            reported: state.watermark,
            queue,
        })
    }

    fn advance(&self, events: Vec<ChangeEvent>, watermark: i64) {
        let limits = self.limits;
        let mut state = self.state.lock().unwrap();
        state.floor.get_or_insert(watermark);
        state.subscribers.retain(|subscriber| {
            subscriber.is_held() && subscriber.offer(events.iter(), watermark, limits)
        });
        state.window_bytes += events.iter().map(ChangeEvent::size_bytes).sum::<usize>();
        state.window.extend(events);
        state.evict_beyond(limits);
        state.watermark = watermark;
        self.applied
            .send_modify(|applied| applied.watermark = watermark);
        drop(state);
        self.ready
            .send_if_modified(|ready| !std::mem::replace(ready, true));
    }

    fn reset(&self) {
        let mut state = self.state.lock().unwrap();
        for subscriber in state.subscribers.drain(..) {
            subscriber.queue.cancel(true);
        }
        state.window.clear();
        state.window_bytes = 0;
        state.floor = None;
        self.applied.send_modify(|applied| applied.resets += 1);
        drop(state);
        self.ready.send_replace(false);
    }
}

impl HubState {
    fn evict_beyond(&mut self, limits: Limits) {
        while self.window.len() > limits.window_events || self.window_bytes > limits.window_bytes {
            let Some(oldest) = self.window.front().map(|event| event.revision) else {
                return;
            };
            while let Some(event) = self.window.pop_front_if(|event| event.revision == oldest) {
                self.window_bytes -= event.size_bytes();
            }
            self.floor = self.floor.max(Some(oldest));
        }
    }
}

impl Subscriber {
    fn is_held(&self) -> bool {
        Arc::strong_count(&self.queue) > 1
    }

    fn wants(&self, event: &ChangeEvent) -> bool {
        event.revision > self.after_revision && self.contains(&event.key)
    }

    fn contains(&self, key: &[u8]) -> bool {
        match self.range_end.as_slice() {
            [] => key == self.key,
            [0] => key >= self.key.as_slice(),
            end => key >= self.key.as_slice() && key < end,
        }
    }

    fn offer<'a>(
        &self,
        events: impl Iterator<Item = &'a ChangeEvent>,
        watermark: i64,
        limits: Limits,
    ) -> bool {
        let wanted: Vec<&ChangeEvent> = events.filter(|event| self.wants(event)).collect();
        // ponytail: every subscriber clones its events, so memory is values times watchers; share them through Arc when fan-out grows
        self.queue.push(
            split_between_revisions(&wanted, watermark, limits.batch_events),
            watermark,
            limits,
        )
    }
}

fn split_between_revisions(
    events: &[&ChangeEvent],
    watermark: i64,
    batch_events: usize,
) -> Vec<Batch> {
    let mut batches = Vec::new();
    let mut current: Vec<ChangeEvent> = Vec::new();
    for revision in events.chunk_by(|a, b| a.revision == b.revision) {
        if let Some(last) = current.last()
            && current.len() + revision.len() > batch_events
        {
            let last_revision = last.revision;
            batches.push(Batch {
                events: std::mem::take(&mut current),
                watermark: last_revision,
            });
        }
        current.extend(revision.iter().map(|&event| event.clone()));
    }
    if !current.is_empty() {
        batches.push(Batch {
            events: current,
            watermark,
        });
    }
    batches
}

impl Queue {
    fn new(watermark: i64) -> Queue {
        Queue {
            state: Mutex::new(QueueState {
                batches: VecDeque::new(),
                bytes: 0,
                watermark,
                cancelled: None,
            }),
            notify: Notify::new(),
        }
    }

    fn push(&self, batches: Vec<Batch>, watermark: i64, limits: Limits) -> bool {
        let mut state = self.state.lock().unwrap();
        let bytes = state.bytes + batches.iter().map(Batch::size_bytes).sum::<usize>();
        if state.batches.len() + batches.len() > limits.subscriber_buffer
            || bytes > limits.subscriber_bytes
        {
            drop(state);
            self.cancel(false);
            return false;
        }
        state.bytes = bytes;
        state.batches.extend(batches);
        state.watermark = watermark;
        drop(state);
        self.notify.notify_one();
        true
    }

    fn cancel(&self, compacted: bool) {
        let mut state = self.state.lock().unwrap();
        state.batches.clear();
        state.bytes = 0;
        state.cancelled = Some(compacted);
        drop(state);
        self.notify.notify_one();
    }
}

impl Subscription {
    pub async fn next(&mut self) -> Delivery {
        loop {
            if let Some(delivery) = self.take() {
                return delivery;
            }
            self.queue.notify.notified().await;
        }
    }

    fn take(&mut self) -> Option<Delivery> {
        let mut state = self.queue.state.lock().unwrap();
        if let Some(compacted) = state.cancelled {
            return Some(Delivery::Cancelled { compacted });
        }
        if let Some(batch) = state.batches.pop_front() {
            state.bytes -= batch.size_bytes();
            self.reported = batch.watermark;
            return Some(Delivery::Events {
                events: batch.events,
                watermark: batch.watermark,
            });
        }
        if state.watermark > self.reported {
            self.reported = state.watermark;
            return Some(Delivery::Progress {
                watermark: state.watermark,
            });
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::task::JoinHandle;

    use super::*;
    use crate::record::Record;

    fn put(key: &str, revision: i64) -> ChangeEvent {
        ChangeEvent {
            key: key.as_bytes().to_vec(),
            revision,
            current: Some(Record {
                version: 1,
                created: revision as u64,
                lease_expiry: 0,
                value: Vec::new(),
            }),
            previous: None,
        }
    }

    fn sized(key: &str, revision: i64, bytes: usize) -> ChangeEvent {
        let mut event = put(key, revision);
        let value_len = bytes - key.len();
        event.current.as_mut().unwrap().value = vec![b'v'; value_len];
        event
    }

    fn unbounded_bytes(window_events: usize, subscriber_buffer: usize) -> Arc<WatchHub> {
        WatchHub::new(window_events, usize::MAX, subscriber_buffer, usize::MAX)
    }

    fn events(events: Vec<ChangeEvent>, watermark: i64) -> FeedOutput {
        FeedOutput::Events { events, watermark }
    }

    fn revisions(events: &[ChangeEvent]) -> Vec<i64> {
        events.iter().map(|event| event.revision).collect()
    }

    fn prefix_a() -> (Vec<u8>, Vec<u8>) {
        (b"a/".to_vec(), b"a0".to_vec())
    }

    fn everything() -> (Vec<u8>, Vec<u8>) {
        (b"\0".to_vec(), b"\0".to_vec())
    }

    async fn next_now(subscription: &mut Subscription) -> Delivery {
        tokio::time::timeout(Duration::from_secs(5), subscription.next())
            .await
            .expect("a delivery is ready")
    }

    async fn nothing_pending(subscription: &mut Subscription) -> bool {
        tokio::time::timeout(Duration::from_millis(50), subscription.next())
            .await
            .is_err()
    }

    async fn events_until(subscription: &mut Subscription, watermark: i64) -> Vec<ChangeEvent> {
        let mut seen = Vec::new();
        loop {
            match next_now(subscription).await {
                Delivery::Events {
                    events,
                    watermark: reached,
                } => {
                    seen.extend(events);
                    if reached >= watermark {
                        return seen;
                    }
                }
                Delivery::Progress { watermark: reached } if reached >= watermark => return seen,
                Delivery::Progress { .. } => {}
                Delivery::Cancelled { compacted } => panic!("cancelled, compacted: {compacted}"),
            }
        }
    }

    #[tokio::test]
    async fn replays_window_then_follows_live() {
        let hub = unbounded_bytes(100, 16);
        hub.apply(events(vec![], 5));
        hub.apply(events(vec![put("a/1", 10), put("a/2", 20)], 25));
        let (key, end) = everything();
        let mut subscription = hub.subscribe(key, end, 5).unwrap();
        hub.apply(events(vec![put("a/3", 30)], 35));

        let seen = events_until(&mut subscription, 35).await;
        assert_eq!(revisions(&seen), vec![10, 20, 30]);
    }

    #[tokio::test]
    async fn future_revision_waits() {
        let hub = unbounded_bytes(100, 16);
        hub.apply(events(vec![], 10));
        let (key, end) = everything();
        let mut subscription = hub.subscribe(key, end, 20).unwrap();
        hub.apply(events(
            vec![put("a/1", 15), put("a/2", 20), put("a/3", 25)],
            30,
        ));

        let seen = events_until(&mut subscription, 30).await;
        assert_eq!(revisions(&seen), vec![25]);
    }

    #[tokio::test]
    async fn range_filter() {
        let hub = unbounded_bytes(100, 16);
        hub.apply(events(vec![], 1));
        let (key, end) = prefix_a();
        let mut subscription = hub.subscribe(key, end, 1).unwrap();
        hub.apply(events(vec![put("a/x", 10), put("b/x", 11)], 12));

        let seen = events_until(&mut subscription, 12).await;
        let keys: Vec<_> = seen.iter().map(|event| event.key.clone()).collect();
        assert_eq!(keys, vec![b"a/x".to_vec()]);
    }

    #[tokio::test]
    async fn single_key_and_from_key_ranges() {
        let hub = unbounded_bytes(100, 16);
        hub.apply(events(vec![], 1));
        let mut single = hub.subscribe(b"a/x".to_vec(), Vec::new(), 1).unwrap();
        let mut from_key = hub.subscribe(b"a/x".to_vec(), vec![0], 1).unwrap();
        hub.apply(events(
            vec![
                put("a/w", 10),
                put("a/x", 11),
                put("a/x0", 12),
                put("z", 13),
            ],
            14,
        ));

        assert_eq!(revisions(&events_until(&mut single, 14).await), vec![11]);
        assert_eq!(
            revisions(&events_until(&mut from_key, 14).await),
            vec![11, 12, 13]
        );
    }

    #[tokio::test]
    async fn too_old_is_compacted() {
        let hub = unbounded_bytes(2, 16);
        let (key, end) = everything();
        assert!(hub.subscribe(key.clone(), end.clone(), 5).is_err());

        hub.apply(events(vec![], 10));
        assert_eq!(
            hub.subscribe(key.clone(), end.clone(), 9).err(),
            Some(Compacted)
        );
        assert!(hub.subscribe(key.clone(), end.clone(), 10).is_ok());

        hub.apply(events(
            vec![put("a/1", 20), put("a/2", 30), put("a/3", 40)],
            45,
        ));
        assert_eq!(
            hub.subscribe(key.clone(), end.clone(), 19).err(),
            Some(Compacted)
        );
        let mut subscription = hub.subscribe(key, end, 20).unwrap();
        assert_eq!(
            revisions(&events_until(&mut subscription, 45).await),
            vec![30, 40]
        );
        assert_eq!(hub.oldest_revision(), 21);
    }

    #[tokio::test]
    async fn slow_subscriber_is_cancelled_and_others_continue() {
        let hub = unbounded_bytes(100, 2);
        hub.apply(events(vec![], 1));
        let (key, end) = everything();
        let mut slow = hub.subscribe(key.clone(), end.clone(), 1).unwrap();
        let mut reader = hub.subscribe(key, end, 1).unwrap();

        let mut seen = Vec::new();
        for revision in 2..12 {
            hub.apply(events(vec![put("a/1", revision)], revision));
            seen.extend(events_until(&mut reader, revision).await);
        }

        assert_eq!(revisions(&seen), (2..12).collect::<Vec<_>>());
        assert_eq!(
            next_now(&mut slow).await,
            Delivery::Cancelled { compacted: false }
        );
    }

    #[tokio::test]
    async fn subscriber_over_its_byte_bound_is_cancelled_and_others_continue() {
        let hub = WatchHub::new(100, usize::MAX, 1024, 250);
        hub.apply(events(vec![], 1));
        let (key, end) = everything();
        let mut slow = hub.subscribe(key.clone(), end.clone(), 1).unwrap();
        let mut reader = hub.subscribe(key, end, 1).unwrap();

        let mut seen = Vec::new();
        for revision in 2..12 {
            hub.apply(events(vec![sized("a/1", revision, 100)], revision));
            seen.extend(events_until(&mut reader, revision).await);
        }

        assert_eq!(revisions(&seen), (2..12).collect::<Vec<_>>());
        assert_eq!(
            next_now(&mut slow).await,
            Delivery::Cancelled { compacted: false }
        );
    }

    #[test]
    fn replay_larger_than_the_subscriber_byte_bound_is_refused_as_compacted() {
        let hub = WatchHub::new(100, usize::MAX, 1024, 250);
        hub.apply(events(vec![], 1));
        hub.apply(events(
            vec![
                sized("a/1", 2, 100),
                sized("a/2", 3, 100),
                sized("a/3", 4, 100),
            ],
            4,
        ));
        let (key, end) = everything();

        assert_eq!(
            hub.subscribe(key.clone(), end.clone(), 1).err(),
            Some(Compacted)
        );
        assert!(hub.subscribe(key, end, 2).is_ok());
    }

    #[test]
    fn replay_larger_than_the_subscriber_batch_bound_is_refused_as_compacted() {
        let hub = WatchHub::with_batch_limit(100, usize::MAX, 2, usize::MAX, 1);
        hub.apply(events(vec![], 1));
        hub.apply(events(vec![put("a/1", 2), put("a/2", 3), put("a/3", 4)], 4));
        let (key, end) = everything();

        assert_eq!(
            hub.subscribe(key.clone(), end.clone(), 1).err(),
            Some(Compacted)
        );
        assert!(hub.subscribe(key, end, 2).is_ok());
    }

    #[tokio::test]
    async fn reset_cancels_everyone_as_compacted() {
        let hub = unbounded_bytes(100, 16);
        hub.apply(events(vec![put("a/1", 5)], 10));
        let (key, end) = everything();
        let mut first = hub.subscribe(key.clone(), end.clone(), 10).unwrap();
        let mut second = hub.subscribe(key.clone(), end.clone(), 10).unwrap();

        hub.apply(FeedOutput::Reset);

        for subscription in [&mut first, &mut second] {
            assert_eq!(
                next_now(subscription).await,
                Delivery::Cancelled { compacted: true }
            );
        }
        assert_eq!(
            hub.subscribe(key.clone(), end.clone(), 10).err(),
            Some(Compacted)
        );
        hub.apply(events(vec![], 50));
        assert_eq!(
            hub.subscribe(key.clone(), end.clone(), 49).err(),
            Some(Compacted)
        );
        assert!(hub.subscribe(key, end, 50).is_ok());
    }

    #[tokio::test]
    async fn progress_reports_watermark_after_queued_events() {
        let hub = unbounded_bytes(100, 16);
        hub.apply(events(vec![], 1));
        let (key, end) = prefix_a();
        let mut subscription = hub.subscribe(key, end, 1).unwrap();
        hub.apply(events(vec![put("a/1", 5)], 6));
        hub.apply(events(vec![put("b/1", 7)], 8));

        assert_eq!(
            next_now(&mut subscription).await,
            Delivery::Events {
                events: vec![put("a/1", 5)],
                watermark: 6
            }
        );
        assert_eq!(
            next_now(&mut subscription).await,
            Delivery::Progress { watermark: 8 }
        );
        assert!(nothing_pending(&mut subscription).await);
    }

    #[tokio::test]
    async fn waits_for_first_watermark() {
        let hub = unbounded_bytes(100, 16);
        let waiter = tokio::spawn({
            let hub = hub.clone();
            async move { hub.wait_for_watermark().await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());
        hub.apply(events(vec![], 3));
        tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .unwrap()
            .unwrap();
    }

    async fn resolves_now(hub: &WatchHub, revision: i64) -> bool {
        tokio::time::timeout(Duration::from_millis(50), hub.delivered(revision))
            .await
            .is_ok()
    }

    fn spawn_delivered(hub: &Arc<WatchHub>, revision: i64) -> JoinHandle<()> {
        let hub = hub.clone();
        tokio::spawn(async move { hub.delivered(revision).await })
    }

    async fn settle() {
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn delivered_resolves_at_once_for_an_already_applied_revision() {
        let hub = unbounded_bytes(100, 16);
        hub.apply(events(vec![put("a/1", 7)], 10));

        assert!(resolves_now(&hub, 7).await);
        assert!(resolves_now(&hub, 10).await);
        assert!(!resolves_now(&hub, 11).await);
    }

    #[tokio::test]
    async fn delivered_waits_until_an_apply_reaches_the_revision() {
        let hub = unbounded_bytes(100, 16);
        hub.apply(events(vec![], 10));
        let waiter = spawn_delivered(&hub, 20);

        settle().await;
        assert!(!waiter.is_finished());
        hub.apply(events(vec![], 19));
        settle().await;
        assert!(!waiter.is_finished());
        hub.apply(events(vec![put("a/1", 20)], 20));
        tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("resolved by the apply that reached it")
            .unwrap();
    }

    #[tokio::test]
    async fn delivered_means_the_events_are_already_queued_to_subscribers() {
        let hub = unbounded_bytes(100, 16);
        hub.apply(events(vec![], 1));
        let (key, end) = everything();
        let mut subscription = hub.subscribe(key, end, 1).unwrap();
        let waiter = spawn_delivered(&hub, 5);
        settle().await;

        hub.apply(events(vec![put("a/1", 5)], 6));
        tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("resolved by the apply")
            .unwrap();

        assert_eq!(
            subscription.take(),
            Some(Delivery::Events {
                events: vec![put("a/1", 5)],
                watermark: 6
            })
        );
    }

    #[tokio::test]
    async fn waiters_at_different_revisions_each_resolve_at_the_apply_that_reaches_them() {
        let hub = unbounded_bytes(100, 16);
        hub.apply(events(vec![], 0));
        let revisions = [3, 17, 8, 30, 8, 1, 25, 12, 30, 21];
        let waiters: Vec<(i64, JoinHandle<()>)> = revisions
            .iter()
            .map(|&revision| (revision, spawn_delivered(&hub, revision)))
            .collect();

        for watermark in [2, 5, 8, 15, 20, 24, 29, 30] {
            hub.apply(events(vec![], watermark));
            settle().await;
            for (revision, waiter) in &waiters {
                assert_eq!(
                    waiter.is_finished(),
                    *revision <= watermark,
                    "waiter at {revision} after applying {watermark}"
                );
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn waiters_racing_concurrent_applies_all_resolve() {
        const LAST: i64 = 5000;
        let hub = unbounded_bytes(100, 16);
        hub.apply(events(vec![], 0));
        let applier = std::thread::spawn({
            let hub = hub.clone();
            move || {
                for watermark in 1..=LAST {
                    hub.apply(events(vec![], watermark));
                }
            }
        });

        let mut waiters = Vec::new();
        let mut revision = 1;
        while !applier.is_finished() && revision <= LAST {
            let target = (hub.watermark() + revision % 7 - 3).clamp(1, LAST);
            waiters.push(spawn_delivered(&hub, target));
            revision += 1;
            tokio::task::yield_now().await;
        }
        applier.join().unwrap();
        assert!(waiters.len() > 10, "only {} waiters", waiters.len());
        for waiter in waiters {
            tokio::time::timeout(Duration::from_secs(5), waiter)
                .await
                .expect("no waiter misses the apply that reaches it")
                .unwrap();
        }
    }

    #[tokio::test]
    async fn a_reset_releases_waiters_because_every_watcher_is_cancelled() {
        let hub = unbounded_bytes(100, 16);
        hub.apply(events(vec![], 10));
        let waiter = spawn_delivered(&hub, 20);
        settle().await;

        hub.apply(FeedOutput::Reset);

        tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("released by the reset")
            .unwrap();
    }

    #[tokio::test]
    async fn events_sharing_a_revision_are_delivered_in_one_batch() {
        let hub = WatchHub::with_batch_limit(100, usize::MAX, 64, usize::MAX, 2);
        hub.apply(events(vec![], 1));
        let (key, end) = everything();
        let mut live = hub.subscribe(key.clone(), end.clone(), 1).unwrap();
        hub.apply(events(
            vec![
                put("a/1", 10),
                put("a/2", 20),
                put("a/3", 20),
                put("a/4", 20),
                put("a/5", 30),
            ],
            30,
        ));
        let mut replayed = hub.subscribe(key, end, 1).unwrap();

        for subscription in [&mut live, &mut replayed] {
            let mut batches = Vec::new();
            while batches.iter().map(Vec::len).sum::<usize>() < 5 {
                if let Delivery::Events { events, .. } = next_now(subscription).await {
                    batches.push(revisions(&events));
                }
            }
            assert!(
                batches.iter().any(|batch| batch == &vec![20, 20, 20]),
                "{batches:?}"
            );
            assert!(batches.iter().all(|batch| batch.len() <= 3), "{batches:?}");
        }
    }

    #[tokio::test]
    async fn replay_respects_after_revision_inside_a_shared_revision() {
        let hub = unbounded_bytes(100, 16);
        hub.apply(events(vec![], 1));
        hub.apply(events(
            vec![
                put("a/1", 10),
                put("a/2", 20),
                put("a/3", 20),
                put("a/4", 30),
            ],
            30,
        ));
        let (key, end) = everything();

        let mut at_20 = hub.subscribe(key.clone(), end.clone(), 20).unwrap();
        assert_eq!(revisions(&events_until(&mut at_20, 30).await), vec![30]);

        let mut at_19 = hub.subscribe(key, end, 19).unwrap();
        assert_eq!(
            revisions(&events_until(&mut at_19, 30).await),
            vec![20, 20, 30]
        );
    }

    #[test]
    fn eviction_drops_whole_revisions() {
        let hub = unbounded_bytes(2, 16);
        hub.apply(events(vec![], 1));
        hub.apply(events(
            vec![put("a/1", 10), put("a/2", 10), put("a/3", 20)],
            20,
        ));
        assert_eq!(hub.oldest_revision(), 11);
    }

    #[test]
    fn window_evicts_by_bytes_on_whole_revision_boundaries() {
        let hub = WatchHub::new(100, 250, 16, usize::MAX);
        hub.apply(events(vec![], 1));
        hub.apply(events(vec![sized("a/0", 5, 10)], 5));
        hub.apply(events(
            vec![
                sized("a/1", 10, 100),
                sized("a/2", 10, 100),
                sized("a/3", 10, 100),
            ],
            10,
        ));

        assert!(hub.state.lock().unwrap().window.is_empty());
        assert_eq!(hub.oldest_revision(), 11);
        let (key, end) = everything();
        assert_eq!(
            hub.subscribe(key.clone(), end.clone(), 9).err(),
            Some(Compacted)
        );
        assert!(hub.subscribe(key, end, 10).is_ok());
    }

    #[test]
    fn window_within_its_byte_bound_keeps_every_revision() {
        let hub = WatchHub::new(100, 300, 16, usize::MAX);
        hub.apply(events(vec![], 1));
        hub.apply(events(
            vec![sized("a/1", 5, 100), sized("a/2", 10, 200)],
            10,
        ));

        assert_eq!(hub.state.lock().unwrap().window.len(), 2);
        assert_eq!(hub.oldest_revision(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn subscribing_between_applies_neither_loses_nor_duplicates() {
        const LAST: i64 = 3000;
        let hub = WatchHub::with_batch_limit(100_000, usize::MAX, 100_000, usize::MAX, 7);
        hub.apply(events(vec![], 0));
        let all: Vec<ChangeEvent> = (1..=LAST)
            .flat_map(|revision| {
                let copies = if revision % 5 == 0 { 3 } else { 1 };
                (0..copies).map(move |n| put(&format!("a/{n}"), revision))
            })
            .collect();

        let applier = std::thread::spawn({
            let hub = hub.clone();
            let all = all.clone();
            move || {
                for revision in 1..=LAST {
                    let batch: Vec<_> = all
                        .iter()
                        .filter(|event| event.revision == revision)
                        .cloned()
                        .collect();
                    hub.apply(events(batch, revision));
                    if revision % 16 == 0 {
                        std::thread::yield_now();
                    }
                }
            }
        });

        let mut readers = Vec::new();
        let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
        while !applier.is_finished() {
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let offset = (seed >> 33) as i64 % 40 - 20;
            let after = (hub.watermark() + offset).max(0);
            let (key, end) = everything();
            let mut subscription = hub.subscribe(key, end, after).unwrap();
            let expected: Vec<_> = all
                .iter()
                .filter(|event| event.revision > after)
                .cloned()
                .collect();
            readers.push(tokio::spawn(async move {
                let seen = events_until(&mut subscription, LAST + 1).await;
                assert_eq!(seen, expected, "subscriber after {after}");
            }));
            tokio::task::yield_now().await;
        }
        applier.join().unwrap();
        hub.apply(events(vec![], LAST + 1));
        assert!(readers.len() > 10, "only {} subscribers", readers.len());
        for reader in readers {
            reader.await.unwrap();
        }
    }
}
