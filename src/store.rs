#![allow(clippy::result_large_err)]

use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use tikv_client::{
    CheckLevel, Key, Snapshot, Timestamp, TimestampExt, Transaction, TransactionClient,
    TransactionOptions,
};

use crate::keys::{data_key, data_range, etcd_key, ttl_key, ttl_range_until};
use crate::record::Record;

const SCAN_PAGE: usize = 1024;
const COMPACT_KEY: &[u8] = b"m/compact";

#[derive(Debug, Clone, PartialEq)]
pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub mod_revision: i64,
    pub create_revision: i64,
    pub version: i64,
    pub lease: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RangeQuery {
    pub key: Vec<u8>,
    pub range_end: Vec<u8>,
    pub limit: i64,
    pub revision: i64,
    pub keys_only: bool,
    pub count_only: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RangeResult {
    pub kvs: Vec<KeyValue>,
    pub more: bool,
    pub count: i64,
    pub revision: i64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Compare {
    ModRevision(i64),
    Version(i64),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Write {
    Put {
        value: Vec<u8>,
        ttl_secs: Option<i64>,
    },
    Delete,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WriteOutcome {
    Applied {
        revision: i64,
        prev: Option<KeyValue>,
    },
    Rejected {
        revision: i64,
        current: Option<KeyValue>,
    },
}

#[derive(Debug)]
pub enum StoreError {
    Compacted,
    FutureRevision,
    Corrupt(Vec<u8>),
    Tikv(tikv_client::Error),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::Compacted => write!(f, "requested revision has been compacted"),
            StoreError::FutureRevision => write!(f, "requested revision is in the future"),
            StoreError::Corrupt(key) => write!(f, "corrupt record for key {key:?}"),
            StoreError::Tikv(error) => write!(f, "tikv: {error}"),
        }
    }
}

impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StoreError::Tikv(error) => Some(error),
            _ => None,
        }
    }
}

impl From<tikv_client::Error> for StoreError {
    fn from(error: tikv_client::Error) -> Self {
        StoreError::Tikv(error)
    }
}

struct Current {
    record: Record,
    mod_revision: i64,
}

impl Current {
    fn key_value(&self, key: &[u8]) -> KeyValue {
        to_key_value(key.to_vec(), self.record.clone(), self.mod_revision)
    }
}

enum Attempt {
    Done(WriteOutcome),
    Conflict,
}

#[derive(Clone)]
pub struct Store {
    client: TransactionClient,
}

impl Store {
    pub async fn connect(pd_endpoints: Vec<String>) -> Result<Store, StoreError> {
        let client = TransactionClient::new(pd_endpoints).await?;
        Ok(Store { client })
    }

    pub fn client(&self) -> &TransactionClient {
        &self.client
    }

    pub async fn now(&self) -> Result<i64, StoreError> {
        Ok(self.client.current_timestamp().await?.version() as i64)
    }

    pub async fn range(&self, q: RangeQuery) -> Result<RangeResult, StoreError> {
        if q.revision != 0 && q.revision < self.compact_revision().await? {
            return Err(StoreError::Compacted);
        }
        let read_at = self.read_timestamp(q.revision).await?;
        let revision = read_at.version() as i64;
        let mut snapshot = self.client.snapshot(read_at, read_options());
        let (start, end) = data_range(&q.key, &q.range_end);

        if q.count_only {
            let count = count_keys(&mut snapshot, start, end).await?;
            return Ok(RangeResult {
                kvs: Vec::new(),
                more: false,
                count,
                revision,
            });
        }

        let limit = usize::try_from(q.limit).ok().filter(|&limit| limit > 0);
        let wanted = limit.map_or(usize::MAX, |limit| limit + 1);
        let mut keys = scan_keys(&mut snapshot, start.clone(), end.clone(), wanted).await?;
        let more = limit.is_some_and(|limit| keys.len() > limit);
        let count = if more {
            count_keys(&mut snapshot, start, end).await?
        } else {
            keys.len() as i64
        };
        if let Some(limit) = limit {
            keys.truncate(limit);
        }

        let mut kvs = fetch_key_values(&mut snapshot, keys).await?;
        if q.keys_only {
            kvs.iter_mut().for_each(|kv| kv.value.clear());
        }
        Ok(RangeResult {
            kvs,
            more,
            count,
            revision,
        })
    }

    pub async fn write(
        &self,
        key: &[u8],
        compare: Option<Compare>,
        write: Write,
    ) -> Result<WriteOutcome, StoreError> {
        loop {
            if let Attempt::Done(outcome) = self.try_write(key, compare, &write).await? {
                return Ok(outcome);
            }
        }
    }

    pub async fn delete_range(
        &self,
        key: &[u8],
        range_end: &[u8],
    ) -> Result<(i64, i64), StoreError> {
        loop {
            if let Some(done) = self.try_delete_range(key, range_end).await? {
                return Ok(done);
            }
        }
    }

    pub async fn expired_ttl_keys(
        &self,
        now_unix_secs: u64,
        limit: u32,
    ) -> Result<Vec<Vec<u8>>, StoreError> {
        let read_at = self.client.current_timestamp().await?;
        let mut snapshot = self.client.snapshot(read_at, read_options());
        let (start, end) = ttl_range_until(now_unix_secs);
        let keys = snapshot.scan_keys(start..end, limit).await?;
        Ok(keys.map(Vec::from).collect())
    }

    pub async fn delete_if_expired(
        &self,
        key: &[u8],
        expiry_unix_secs: u64,
    ) -> Result<bool, StoreError> {
        loop {
            if let Some(deleted) = self.try_delete_if_expired(key, expiry_unix_secs).await? {
                return Ok(deleted);
            }
        }
    }

    pub async fn compact(&self, revision: i64) -> Result<i64, StoreError> {
        if revision <= 0 {
            return Err(StoreError::Compacted);
        }
        if revision > self.now().await? {
            return Err(StoreError::FutureRevision);
        }
        let (previous, committed) = loop {
            if let Some(raised) = self.try_raise_compaction(revision).await? {
                break raised;
            }
        };
        if previous > 0 {
            // ponytail: gc resolves locks across the whole keyspace before moving the safepoint; scope it to k/ if that gets slow
            self.client
                .gc(Timestamp::from_version(previous as u64))
                .await?;
        }
        Ok(committed)
    }

    pub async fn compact_revision(&self) -> Result<i64, StoreError> {
        let read_at = self.client.current_timestamp().await?;
        let mut snapshot = self.client.snapshot(read_at, read_options());
        decode_compact_revision(snapshot.get(COMPACT_KEY.to_vec()).await?)
    }

    async fn read_timestamp(&self, revision: i64) -> Result<Timestamp, StoreError> {
        if revision == 0 {
            return Ok(self.client.current_timestamp().await?);
        }
        Ok(Timestamp::from_version(revision as u64))
    }

    async fn begin_write(&self) -> Result<Transaction, StoreError> {
        let options = TransactionOptions::new_optimistic().drop_check(CheckLevel::None);
        Ok(self.client.begin_with_options(options).await?)
    }

    async fn try_write(
        &self,
        key: &[u8],
        compare: Option<Compare>,
        write: &Write,
    ) -> Result<Attempt, StoreError> {
        let mut txn = self.begin_write().await?;
        let read_revision = txn.start_timestamp().version() as i64;
        let current = read_current(&mut txn, key).await?;

        if !compare.is_none_or(|compare| compare_holds(compare, current.as_ref())) {
            return Ok(Attempt::Done(WriteOutcome::Rejected {
                revision: read_revision,
                current: current.map(|current| current.key_value(key)),
            }));
        }

        let prev_record = current.as_ref().map(|current| &current.record);
        match write {
            Write::Put { value, ttl_secs } => {
                let record = next_record(prev_record, value, *ttl_secs, txn.start_timestamp());
                stage_ttl_index(&mut txn, key, prev_record, record.lease_expiry).await?;
                txn.put(data_key(key), record.encode()).await?;
            }
            Write::Delete if current.is_none() => {
                return Ok(Attempt::Done(WriteOutcome::Applied {
                    revision: read_revision,
                    prev: None,
                }));
            }
            Write::Delete => stage_delete(&mut txn, key, prev_record).await?,
        }

        let Some(revision) = commit_unless_conflicted(&mut txn).await? else {
            return Ok(Attempt::Conflict);
        };
        Ok(Attempt::Done(WriteOutcome::Applied {
            revision,
            prev: current.map(|current| current.key_value(key)),
        }))
    }

    async fn try_raise_compaction(&self, revision: i64) -> Result<Option<(i64, i64)>, StoreError> {
        let mut txn = self.begin_write().await?;
        let previous = decode_compact_revision(txn.get(COMPACT_KEY.to_vec()).await?)?;
        if revision <= previous {
            return Err(StoreError::Compacted);
        }
        txn.put(COMPACT_KEY.to_vec(), revision.to_be_bytes().to_vec())
            .await?;
        Ok(commit_unless_conflicted(&mut txn)
            .await?
            .map(|committed| (previous, committed)))
    }

    async fn try_delete_if_expired(
        &self,
        key: &[u8],
        expiry_unix_secs: u64,
    ) -> Result<Option<bool>, StoreError> {
        let mut txn = self.begin_write().await?;
        let current = read_current(&mut txn, key).await?;
        let expired = current
            .as_ref()
            .is_some_and(|current| current.record.lease_expiry == expiry_unix_secs);
        if expired {
            stage_delete(&mut txn, key, current.as_ref().map(|c| &c.record)).await?;
        } else {
            txn.delete(ttl_key(expiry_unix_secs, key)).await?;
        }
        Ok(commit_unless_conflicted(&mut txn).await?.map(|_| expired))
    }

    async fn try_delete_range(
        &self,
        key: &[u8],
        range_end: &[u8],
    ) -> Result<Option<(i64, i64)>, StoreError> {
        let mut txn = self.begin_write().await?;
        let read_revision = txn.start_timestamp().version() as i64;
        let mut snapshot = self.client.snapshot(txn.start_timestamp(), read_options());
        let (start, end) = data_range(key, range_end);
        let keys = scan_keys(&mut snapshot, start, end, usize::MAX).await?;
        if keys.is_empty() {
            return Ok(Some((0, read_revision)));
        }

        let deleted = keys.len() as i64;
        for pair in snapshot.batch_get(keys).await? {
            let etcd = etcd_key_of(pair.key())?.to_vec();
            let record = decode(&etcd, pair.value())?;
            stage_delete(&mut txn, &etcd, Some(&record)).await?;
        }

        Ok(commit_unless_conflicted(&mut txn)
            .await?
            .map(|revision| (deleted, revision)))
    }
}

fn read_options() -> TransactionOptions {
    TransactionOptions::new_optimistic()
        .read_only()
        .drop_check(CheckLevel::None)
}

async fn read_current(txn: &mut Transaction, key: &[u8]) -> Result<Option<Current>, StoreError> {
    let Some((bytes, commit_ts)) = txn.get_with_commit_ts(data_key(key)).await? else {
        return Ok(None);
    };
    Ok(Some(Current {
        record: decode(key, &bytes)?,
        mod_revision: commit_ts.version() as i64,
    }))
}

fn decode_compact_revision(bytes: Option<Vec<u8>>) -> Result<i64, StoreError> {
    let Some(bytes) = bytes else {
        return Ok(0);
    };
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| StoreError::Corrupt(COMPACT_KEY.to_vec()))?;
    Ok(i64::from_be_bytes(bytes))
}

fn compare_holds(compare: Compare, current: Option<&Current>) -> bool {
    match compare {
        Compare::ModRevision(expected) => {
            current.map_or(0, |current| current.mod_revision) == expected
        }
        Compare::Version(expected) => {
            current.map_or(0, |current| current.record.version) == expected
        }
    }
}

fn next_record(
    prev: Option<&Record>,
    value: &[u8],
    ttl_secs: Option<i64>,
    start_ts: Timestamp,
) -> Record {
    Record {
        version: prev.map_or(0, |prev| prev.version) + 1,
        created: prev.map_or(start_ts.version(), |prev| prev.created),
        lease_expiry: ttl_secs.map_or(0, expiry_after),
        value: value.to_vec(),
    }
}

fn expiry_after(ttl_secs: i64) -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs());
    now.saturating_add_signed(ttl_secs)
}

async fn stage_ttl_index(
    txn: &mut Transaction,
    key: &[u8],
    prev: Option<&Record>,
    new_expiry: u64,
) -> Result<(), StoreError> {
    let prev_expiry = prev.map_or(0, |prev| prev.lease_expiry);
    if prev_expiry == new_expiry {
        return Ok(());
    }
    if prev_expiry != 0 {
        txn.delete(ttl_key(prev_expiry, key)).await?;
    }
    if new_expiry != 0 {
        txn.put(ttl_key(new_expiry, key), Vec::new()).await?;
    }
    Ok(())
}

async fn stage_delete(
    txn: &mut Transaction,
    key: &[u8],
    prev: Option<&Record>,
) -> Result<(), StoreError> {
    stage_ttl_index(txn, key, prev, 0).await?;
    txn.delete(data_key(key)).await?;
    Ok(())
}

async fn commit_unless_conflicted(txn: &mut Transaction) -> Result<Option<i64>, StoreError> {
    match txn.commit().await {
        Ok(Some(commit_ts)) => Ok(Some(commit_ts.version() as i64)),
        Ok(None) => Err(StoreError::Tikv(tikv_client::Error::ProtocolViolation {
            message: "commit of a non-empty write returned no commit timestamp".to_owned(),
        })),
        Err(error) => {
            let _ = txn.rollback().await;
            if is_write_conflict(&error) {
                Ok(None)
            } else {
                Err(error.into())
            }
        }
    }
}

fn is_write_conflict(error: &tikv_client::Error) -> bool {
    match error {
        tikv_client::Error::KeyError(key_error) => key_error.conflict.is_some(),
        tikv_client::Error::MultipleKeyErrors(errors) => errors.iter().any(is_write_conflict),
        _ => false,
    }
}

async fn scan_keys(
    snapshot: &mut Snapshot,
    start: Vec<u8>,
    end: Vec<u8>,
    wanted: usize,
) -> Result<Vec<Key>, StoreError> {
    let mut keys = Vec::new();
    let mut from = start;
    while keys.len() < wanted {
        let page = (wanted - keys.len()).min(SCAN_PAGE);
        let batch: Vec<Key> = snapshot
            .scan_keys(from.clone()..end.clone(), page as u32)
            .await?
            .collect();
        let exhausted = batch.len() < page;
        if let Some(last) = batch.last() {
            from = key_after(last);
        }
        keys.extend(batch);
        if exhausted {
            break;
        }
    }
    Ok(keys)
}

// ponytail: exact count rescans keys per page, estimate if lists get slow
async fn count_keys(
    snapshot: &mut Snapshot,
    start: Vec<u8>,
    end: Vec<u8>,
) -> Result<i64, StoreError> {
    Ok(scan_keys(snapshot, start, end, usize::MAX).await?.len() as i64)
}

fn key_after(key: &Key) -> Vec<u8> {
    let bytes: &[u8] = key.into();
    [bytes, &[0]].concat()
}

async fn fetch_key_values(
    snapshot: &mut Snapshot,
    keys: Vec<Key>,
) -> Result<Vec<KeyValue>, StoreError> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let mut kvs = snapshot
        .batch_get_with_commit_ts(keys)
        .await?
        .into_iter()
        .map(|(pair, commit_ts)| {
            let etcd = etcd_key_of(pair.key())?.to_vec();
            let record = decode(&etcd, pair.value())?;
            Ok(to_key_value(etcd, record, commit_ts.version() as i64))
        })
        .collect::<Result<Vec<_>, StoreError>>()?;
    kvs.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(kvs)
}

fn etcd_key_of(key: &Key) -> Result<&[u8], StoreError> {
    let bytes: &[u8] = key.into();
    etcd_key(bytes).ok_or_else(|| StoreError::Corrupt(bytes.to_vec()))
}

fn decode(key: &[u8], bytes: &[u8]) -> Result<Record, StoreError> {
    Record::decode(bytes).map_err(|_| StoreError::Corrupt(key.to_vec()))
}

fn to_key_value(key: Vec<u8>, record: Record, mod_revision: i64) -> KeyValue {
    let create_revision = if record.version == 1 {
        mod_revision
    } else {
        record.created as i64
    };
    KeyValue {
        key,
        value: record.value,
        mod_revision,
        create_revision,
        version: record.version,
        lease: record.lease_expiry as i64,
    }
}
