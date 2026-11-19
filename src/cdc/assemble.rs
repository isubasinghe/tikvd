use std::collections::HashMap;

use tikv_client::proto::cdcpb::event::row::OpType;
use tikv_client::proto::cdcpb::event::{LogType, Row};

use crate::keys;
use crate::record::{CorruptRecord, Record};

#[derive(Debug, Clone, PartialEq)]
pub struct ChangeEvent {
    pub key: Vec<u8>,
    pub revision: i64,
    pub current: Option<Record>,
    pub previous: Option<Record>,
}

impl ChangeEvent {
    pub fn size_bytes(&self) -> usize {
        let value_len = |record: &Option<Record>| record.as_ref().map_or(0, |r| r.value.len());
        self.key.len() + value_len(&self.current) + value_len(&self.previous)
    }
}

struct HeldPrewrite {
    value: Vec<u8>,
    old_value: Vec<u8>,
    op_type: i32,
    first_resolved_after: Option<ResolvedBy>,
}

struct ResolvedBy {
    registration: u64,
    resolved_ts: u64,
}

#[derive(Default)]
pub struct Assembler {
    // ponytail: held prewrites are not counted against the sorter's memory limit; count them there if large uncommitted writes pile up
    prewrites: HashMap<(Vec<u8>, u64), HeldPrewrite>,
}

impl Assembler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn on_row(&mut self, row: Row) -> Result<Option<ChangeEvent>, CorruptRecord> {
        if keys::etcd_key(&row.key).is_none() {
            return Ok(None);
        }
        match LogType::try_from(row.r#type) {
            Ok(LogType::Committed) => {
                self.prewrites.remove(&(row.key.clone(), row.start_ts));
                build_event(
                    &row.key,
                    row.commit_ts,
                    &row.value,
                    &row.old_value,
                    row.op_type,
                )
            }
            Ok(LogType::Prewrite) => {
                let held = HeldPrewrite {
                    value: row.value,
                    old_value: row.old_value,
                    op_type: row.op_type,
                    first_resolved_after: None,
                };
                self.prewrites.insert((row.key, row.start_ts), held);
                Ok(None)
            }
            Ok(LogType::Commit) => match self.prewrites.remove(&(row.key.clone(), row.start_ts)) {
                Some(held) => build_event(
                    &row.key,
                    row.commit_ts,
                    &held.value,
                    &held.old_value,
                    held.op_type,
                ),
                None => Ok(None),
            },
            Ok(LogType::Rollback) => {
                self.prewrites.remove(&(row.key, row.start_ts));
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    pub fn is_unmatched_commit(&self, row: &Row) -> bool {
        row.r#type == LogType::Commit as i32
            && keys::etcd_key(&row.key).is_some()
            && !self
                .prewrites
                .contains_key(&(row.key.clone(), row.start_ts))
    }

    pub fn on_resolved_ts(
        &mut self,
        range: &(Vec<u8>, Vec<u8>),
        registration: u64,
        resolved_ts: u64,
    ) {
        let (start, end) = range;
        self.prewrites.retain(|(key, start_ts), held| {
            if key < start || key >= end {
                return true;
            }
            match &held.first_resolved_after {
                Some(first) if first.registration == registration => {
                    resolved_ts <= *start_ts || resolved_ts <= first.resolved_ts
                }
                _ => {
                    held.first_resolved_after = Some(ResolvedBy {
                        registration,
                        resolved_ts,
                    });
                    true
                }
            }
        });
    }
}

fn build_event(
    raw_key: &[u8],
    commit_ts: u64,
    value: &[u8],
    old_value: &[u8],
    op_type: i32,
) -> Result<Option<ChangeEvent>, CorruptRecord> {
    let Some(key) = keys::etcd_key(raw_key) else {
        return Ok(None);
    };
    let is_delete = op_type == OpType::Delete as i32;
    let current = if is_delete || value.is_empty() {
        None
    } else {
        Some(Record::decode(value)?)
    };
    let previous = if old_value.is_empty() {
        None
    } else {
        Some(Record::decode(old_value)?)
    };
    Ok(Some(ChangeEvent {
        key: key.to_vec(),
        revision: commit_ts as i64,
        current,
        previous,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(version: i64, value: &[u8]) -> Record {
        Record {
            version,
            created: 1,
            lease_expiry: 0,
            value: value.to_vec(),
        }
    }

    fn row(log_type: LogType, op: OpType, key: &[u8], start_ts: u64, commit_ts: u64) -> Row {
        Row {
            r#type: log_type as i32,
            op_type: op as i32,
            key: keys::data_key(key),
            start_ts,
            commit_ts,
            ..Default::default()
        }
    }

    fn with_values(mut row: Row, value: Option<&Record>, old: Option<&Record>) -> Row {
        row.value = value.map(Record::encode).unwrap_or_default();
        row.old_value = old.map(Record::encode).unwrap_or_default();
        row
    }

    #[test]
    fn committed_row_is_an_event() {
        let current = record(2, b"v2");
        let previous = record(1, b"v1");
        let committed = with_values(
            row(LogType::Committed, OpType::Put, b"a", 5, 9),
            Some(&current),
            Some(&previous),
        );
        let event = Assembler::new().on_row(committed).unwrap().unwrap();
        assert_eq!(
            event,
            ChangeEvent {
                key: b"a".to_vec(),
                revision: 9,
                current: Some(current),
                previous: Some(previous)
            }
        );
    }

    #[test]
    fn prewrite_then_commit_joins() {
        let current = record(2, b"v2");
        let previous = record(1, b"v1");
        let mut assembler = Assembler::new();
        let prewrite = with_values(
            row(LogType::Prewrite, OpType::Put, b"a", 5, 0),
            Some(&current),
            Some(&previous),
        );
        assert_eq!(assembler.on_row(prewrite).unwrap(), None);
        let event = assembler
            .on_row(row(LogType::Commit, OpType::Put, b"a", 5, 12))
            .unwrap()
            .unwrap();
        assert_eq!(event.revision, 12);
        assert_eq!(event.current, Some(current));
        assert_eq!(event.previous, Some(previous));
    }

    #[test]
    fn rollback_drops_prewrite() {
        let mut assembler = Assembler::new();
        let prewrite = with_values(
            row(LogType::Prewrite, OpType::Put, b"a", 5, 0),
            Some(&record(1, b"v")),
            None,
        );
        assembler.on_row(prewrite).unwrap();
        let rollback = row(LogType::Rollback, OpType::Put, b"a", 5, 0);
        assert_eq!(assembler.on_row(rollback).unwrap(), None);
        let commit = row(LogType::Commit, OpType::Put, b"a", 5, 12);
        assert_eq!(assembler.on_row(commit).unwrap(), None);
    }

    #[test]
    fn commit_without_prewrite_is_none() {
        let mut assembler = Assembler::new();
        let commit = row(LogType::Commit, OpType::Put, b"a", 5, 12);
        assert_eq!(assembler.on_row(commit).unwrap(), None);
    }

    #[test]
    fn no_old_value_is_create() {
        let committed = with_values(
            row(LogType::Committed, OpType::Put, b"a", 5, 9),
            Some(&record(1, b"v")),
            None,
        );
        let event = Assembler::new().on_row(committed).unwrap().unwrap();
        assert_eq!(event.previous, None);
        assert!(event.current.is_some());
    }

    #[test]
    fn delete_row_has_no_current_and_has_previous() {
        let previous = record(1, b"v1");
        let committed = with_values(
            row(LogType::Committed, OpType::Delete, b"a", 5, 9),
            None,
            Some(&previous),
        );
        let event = Assembler::new().on_row(committed).unwrap().unwrap();
        assert_eq!(event.current, None);
        assert_eq!(event.previous, Some(previous));
    }

    #[test]
    fn two_phase_delete_takes_old_value_from_prewrite() {
        let previous = record(1, b"v1");
        let mut assembler = Assembler::new();
        let prewrite = with_values(
            row(LogType::Prewrite, OpType::Delete, b"a", 5, 0),
            None,
            Some(&previous),
        );
        assembler.on_row(prewrite).unwrap();
        let event = assembler
            .on_row(row(LogType::Commit, OpType::Delete, b"a", 5, 12))
            .unwrap()
            .unwrap();
        assert_eq!(event.current, None);
        assert_eq!(event.previous, Some(previous));
    }

    #[test]
    fn key_outside_data_prefix_is_ignored() {
        let mut other = row(LogType::Committed, OpType::Put, b"a", 5, 9);
        other.key = b"l/a".to_vec();
        assert_eq!(Assembler::new().on_row(other).unwrap(), None);
    }

    #[test]
    fn initialized_row_is_none() {
        let initialized = row(LogType::Initialized, OpType::Put, b"a", 0, 0);
        assert_eq!(Assembler::new().on_row(initialized).unwrap(), None);
    }

    #[test]
    fn corrupt_value_is_an_error() {
        let mut committed = row(LogType::Committed, OpType::Put, b"a", 5, 9);
        committed.value = vec![1, 2, 3];
        assert_eq!(Assembler::new().on_row(committed), Err(CorruptRecord));
    }

    #[test]
    fn committed_row_clears_a_held_prewrite() {
        let mut assembler = Assembler::new();
        let prewrite = with_values(
            row(LogType::Prewrite, OpType::Put, b"a", 5, 0),
            Some(&record(1, b"v")),
            None,
        );
        assembler.on_row(prewrite).unwrap();
        let committed = with_values(
            row(LogType::Committed, OpType::Put, b"a", 5, 9),
            Some(&record(1, b"v")),
            None,
        );
        assert!(assembler.on_row(committed).unwrap().is_some());
        let commit = row(LogType::Commit, OpType::Put, b"a", 5, 9);
        assert!(assembler.is_unmatched_commit(&commit));
        assert_eq!(assembler.on_row(commit).unwrap(), None);
    }

    fn held_prewrite(assembler: &mut Assembler, key: &[u8], start_ts: u64) {
        let prewrite = with_values(
            row(LogType::Prewrite, OpType::Put, key, start_ts, 0),
            Some(&record(1, b"v")),
            None,
        );
        assembler.on_row(prewrite).unwrap();
    }

    fn holds(assembler: &Assembler, key: &[u8], start_ts: u64) -> bool {
        !assembler.is_unmatched_commit(&row(LogType::Commit, OpType::Put, key, start_ts, 99))
    }

    fn whole() -> (Vec<u8>, Vec<u8>) {
        (b"k/".to_vec(), b"k0".to_vec())
    }

    #[test]
    fn a_prewrite_is_dropped_by_a_second_resolved_ts_above_its_start_and_the_first() {
        let mut assembler = Assembler::new();
        held_prewrite(&mut assembler, b"a", 50);
        assembler.on_resolved_ts(&whole(), 1, 60);
        assert!(holds(&assembler, b"a", 50));
        assembler.on_resolved_ts(&whole(), 1, 70);
        assert!(!holds(&assembler, b"a", 50));
    }

    #[test]
    fn repeated_resolved_ts_above_a_late_prewrite_does_not_drop_it() {
        let mut assembler = Assembler::new();
        held_prewrite(&mut assembler, b"a", 50);
        for _ in 0..10 {
            assembler.on_resolved_ts(&whole(), 1, 100);
        }
        assert!(holds(&assembler, b"a", 50));
    }

    #[test]
    fn a_resolved_ts_at_or_below_the_start_does_not_drop_a_prewrite() {
        let mut assembler = Assembler::new();
        held_prewrite(&mut assembler, b"a", 50);
        assembler.on_resolved_ts(&whole(), 1, 40);
        assembler.on_resolved_ts(&whole(), 1, 50);
        assert!(holds(&assembler, b"a", 50));
        assembler.on_resolved_ts(&whole(), 1, 51);
        assert!(!holds(&assembler, b"a", 50));
    }

    #[test]
    fn another_registration_restarts_the_count() {
        let mut assembler = Assembler::new();
        held_prewrite(&mut assembler, b"a", 50);
        assembler.on_resolved_ts(&whole(), 1, 60);
        assembler.on_resolved_ts(&whole(), 2, 70);
        assert!(holds(&assembler, b"a", 50));
        assembler.on_resolved_ts(&whole(), 2, 80);
        assert!(!holds(&assembler, b"a", 50));
    }

    #[test]
    fn a_resolved_ts_for_another_range_leaves_a_prewrite_alone() {
        let mut assembler = Assembler::new();
        held_prewrite(&mut assembler, b"z", 50);
        let left = (keys::data_key(b"a"), keys::data_key(b"m"));
        assembler.on_resolved_ts(&left, 1, 60);
        assembler.on_resolved_ts(&left, 1, 70);
        assert!(holds(&assembler, b"z", 50));
    }

    #[test]
    fn only_a_commit_with_no_held_prewrite_under_the_data_prefix_is_unmatched() {
        let mut assembler = Assembler::new();
        let prewrite = with_values(
            row(LogType::Prewrite, OpType::Put, b"a", 5, 0),
            Some(&record(1, b"v")),
            None,
        );
        assembler.on_row(prewrite).unwrap();
        assert!(!assembler.is_unmatched_commit(&row(LogType::Commit, OpType::Put, b"a", 5, 9)));
        assert!(assembler.is_unmatched_commit(&row(LogType::Commit, OpType::Put, b"b", 5, 9)));
        let mut outside = row(LogType::Commit, OpType::Put, b"b", 5, 9);
        outside.key = b"l/b".to_vec();
        assert!(!assembler.is_unmatched_commit(&outside));
        assert!(!assembler.is_unmatched_commit(&row(LogType::Committed, OpType::Put, b"b", 5, 9)));
    }
}
