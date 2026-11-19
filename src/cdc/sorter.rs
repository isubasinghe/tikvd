use std::collections::{BTreeMap, HashMap, HashSet};

use super::assemble::ChangeEvent;

#[derive(Debug, PartialEq)]
pub struct SorterFull;

pub struct Sorter {
    memory_limit_bytes: usize,
    memory_used_bytes: usize,
    held: BTreeMap<(i64, Vec<u8>), ChangeEvent>,
    required_regions: HashSet<u64>,
    resolved: HashMap<u64, u64>,
    watermark: u64,
}

impl Sorter {
    pub fn new(memory_limit_bytes: usize) -> Self {
        Self {
            memory_limit_bytes,
            memory_used_bytes: 0,
            held: BTreeMap::new(),
            required_regions: HashSet::new(),
            resolved: HashMap::new(),
            watermark: 0,
        }
    }

    pub fn set_regions(&mut self, region_ids: &[u64]) {
        self.required_regions = region_ids.iter().copied().collect();
        self.resolved
            .retain(|region, _| self.required_regions.contains(region));
    }

    pub fn set_resolved(&mut self, region_id: u64, ts: u64) {
        if !self.required_regions.contains(&region_id) {
            return;
        }
        let current = self.resolved.entry(region_id).or_insert(0);
        *current = (*current).max(ts);
    }

    pub fn push(&mut self, event: ChangeEvent) -> Result<(), SorterFull> {
        if event.revision as u64 <= self.watermark {
            return Ok(());
        }
        let id = (event.revision, event.key.clone());
        if self.held.contains_key(&id) {
            return Ok(());
        }
        let size = event.size_bytes();
        if self.memory_used_bytes + size > self.memory_limit_bytes {
            return Err(SorterFull);
        }
        self.memory_used_bytes += size;
        self.held.insert(id, event);
        Ok(())
    }

    pub fn drain(&mut self) -> (Vec<ChangeEvent>, i64) {
        self.watermark = self.watermark.max(self.minimum_resolved());
        let watermark = i64::try_from(self.watermark).unwrap_or(i64::MAX);
        let kept = self
            .held
            .split_off(&(watermark.saturating_add(1), Vec::new()));
        let released = std::mem::replace(&mut self.held, kept);
        let events: Vec<ChangeEvent> = released.into_values().collect();
        let released_bytes: usize = events.iter().map(ChangeEvent::size_bytes).sum();
        self.memory_used_bytes -= released_bytes;
        (events, watermark)
    }

    fn minimum_resolved(&self) -> u64 {
        self.required_regions
            .iter()
            .map(|region| self.resolved.get(region).copied().unwrap_or(0))
            .min()
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(revision: i64) -> ChangeEvent {
        ChangeEvent {
            key: b"k".to_vec(),
            revision,
            current: None,
            previous: None,
        }
    }

    fn revisions(events: &[ChangeEvent]) -> Vec<i64> {
        events.iter().map(|e| e.revision).collect()
    }

    #[test]
    fn drain_orders_by_revision_across_regions() {
        let mut sorter = Sorter::new(1 << 20);
        sorter.set_regions(&[1, 2]);
        for revision in [30, 10, 20] {
            sorter.push(event(revision)).unwrap();
        }
        sorter.set_resolved(1, 25);
        sorter.set_resolved(2, 25);
        let (events, watermark) = sorter.drain();
        assert_eq!(revisions(&events), vec![10, 20]);
        assert_eq!(watermark, 25);
        sorter.set_resolved(1, 40);
        sorter.set_resolved(2, 40);
        assert_eq!(revisions(&sorter.drain().0), vec![30]);
    }

    #[test]
    fn unresolved_region_holds_watermark() {
        let mut sorter = Sorter::new(1 << 20);
        sorter.set_regions(&[1, 2]);
        sorter.push(event(60)).unwrap();
        sorter.set_resolved(1, 100);
        assert_eq!(sorter.drain(), (Vec::new(), 0));

        sorter.set_resolved(2, 50);
        sorter.set_resolved(1, 200);
        let (events, watermark) = sorter.drain();
        assert!(events.is_empty());
        assert_eq!(watermark, 50);
    }

    #[test]
    fn a_region_dropped_from_the_set_does_not_hold_the_watermark() {
        let mut sorter = Sorter::new(1 << 20);
        sorter.set_regions(&[1, 2]);
        sorter.set_resolved(1, 50);
        sorter.set_resolved(2, 50);
        sorter.drain();
        sorter.set_regions(&[1, 3]);
        sorter.set_resolved(1, 100);
        sorter.set_resolved(3, 100);
        sorter.set_resolved(1, 500);
        sorter.set_resolved(3, 500);
        assert_eq!(sorter.drain().1, 500);
    }

    #[test]
    fn watermark_never_decreases() {
        let mut sorter = Sorter::new(1 << 20);
        sorter.set_regions(&[1]);
        sorter.set_resolved(1, 50);
        assert_eq!(sorter.drain().1, 50);
        sorter.set_regions(&[1, 2]);
        assert_eq!(sorter.drain().1, 50);
        sorter.set_resolved(1, 10);
        assert_eq!(sorter.drain().1, 50);
    }

    #[test]
    fn resolved_for_region_outside_set_is_ignored() {
        let mut sorter = Sorter::new(1 << 20);
        sorter.set_regions(&[1]);
        sorter.set_resolved(2, 90);
        sorter.set_resolved(1, 5);
        assert_eq!(sorter.drain().1, 5);
    }

    #[test]
    fn replayed_event_is_deduplicated() {
        let mut sorter = Sorter::new(1 << 20);
        sorter.set_regions(&[1]);
        sorter.push(event(7)).unwrap();
        sorter.push(event(7)).unwrap();
        sorter.set_resolved(1, 10);
        assert_eq!(revisions(&sorter.drain().0), vec![7]);
    }

    #[test]
    fn event_at_or_below_watermark_after_reregister_is_dropped() {
        let mut sorter = Sorter::new(1 << 20);
        sorter.set_regions(&[1]);
        sorter.set_resolved(1, 10);
        sorter.drain();
        sorter.push(event(5)).unwrap();
        sorter.push(event(10)).unwrap();
        sorter.set_resolved(1, 20);
        assert!(sorter.drain().0.is_empty());
    }

    #[test]
    fn same_revision_orders_by_key() {
        let mut sorter = Sorter::new(1 << 20);
        sorter.set_regions(&[1]);
        for key in [b"b", b"a"] {
            sorter
                .push(ChangeEvent {
                    key: key.to_vec(),
                    ..event(3)
                })
                .unwrap();
        }
        sorter.set_resolved(1, 3);
        let keys: Vec<Vec<u8>> = sorter.drain().0.into_iter().map(|e| e.key).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);
    }

    #[test]
    fn over_limit_is_sorter_full() {
        let mut sorter = Sorter::new(1);
        assert_eq!(sorter.push(event(1)), Ok(()));
        assert_eq!(sorter.push(event(2)), Err(SorterFull));
    }

    #[test]
    fn draining_frees_memory() {
        let mut sorter = Sorter::new(1);
        sorter.set_regions(&[1]);
        sorter.push(event(1)).unwrap();
        sorter.set_resolved(1, 1);
        sorter.drain();
        assert_eq!(sorter.push(event(2)), Ok(()));
    }
}
