# tikvd (etcd on TiKV) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A stateless Rust server that speaks kube-apiserver's subset of the etcd v3 gRPC API and stores everything in TiKV, proven by k3s plus Argo Workflows running on two instances.

**Architecture:** etcd revisions are TiKV commit timestamps. writes are optimistic two-phase TiKV transactions; reads are snapshot reads that ask TiKV for each key's commit timestamp. Each instance tails TiKV's CDC stream, orders events by commit timestamp, and releases them to watchers only up to the resolved-timestamp watermark.

**Tech Stack:** Rust 2024, tokio, tonic 0.12 / prost 0.13 (matching the vendored `tikv-client` 0.4.0), `clap` for flags, `etcd-client` as a dev-dependency to drive the server in tests, Docker Compose for a local PD + TiKV.

**Spec:** `docs/superpowers/specs/2026-11-19-etcd-on-tikv-design.md`

## Global Constraints

- Revision = TiKV commit timestamp, as `i64`. Never derive a revision from a start timestamp or a mid-transaction timestamp.
- Never deliver a watch event above the watermark, and never out of commit-timestamp order.
- No instance-local durable state. No coordination between instances.
- Anything kube-apiserver does not send returns gRPC `UNIMPLEMENTED`; unsupported `Txn` shapes return `UNIMPLEMENTED` with the shape in the message.
- All TiKV keys are under `k/` (data), `l/` (TTL index) or `m/` (metadata: `m/compact`).
- `Status` reports etcd version `3.5.13`.
- Source carries no explanatory comments (user's global rule). A one-line comment is allowed only for an external constraint the code cannot express; deliberate shortcuts get a one-line `ponytail:` marker naming the ceiling.
- Add no dependency beyond the Tech Stack line without a reason stated in the commit message.
- Integration tests need a TiKV cluster. They read `TIKVD_TEST_PD` (for example `127.0.0.1:2379`) and are skipped with a printed notice when it is unset.
- Commit steps run only once the user has said commits are wanted for this work; otherwise leave changes staged and say so.

## Review Focus

1. Two clients create the same key at once: exactly one gets `succeeded = true`, and the loser's retry re-evaluates the compare and returns the winner's value. (Task 3)
2. A watch starts at a revision above the instance's current watermark (the normal list-then-watch case): it waits and then delivers every event above that revision, none below. (Task 7)
3. A region covering the range has no live registration: the watermark does not advance, even if every other region's resolved timestamp does. (Task 5)
4. A watcher that stops reading: it is cancelled; it never blocks other watchers or the CDC consumer. (Task 7)
5. Keys at the edges of a range: `range_end = "\0"` (to end of keyspace), a prefix whose last byte is `0xff`, and an empty value. (Tasks 2, 3)

---

### Task 0: Spike (throwaway, gates everything after it)

**Files:**
- Create: `dev/docker-compose.yml`
- Create (then delete): `spike/` (its own Cargo project, never part of the workspace)
- Modify: `docs/superpowers/specs/2026-11-19-etcd-on-tikv-design.md` ("Facts this design rests on")

- [ ] **Step 1:** Write `dev/docker-compose.yml` with one `pingcap/pd` and one `pingcap/tikv` container on host networking (PD client on `127.0.0.1:2379`, TiKV on `127.0.0.1:20160`), TiKV started with `cdc.min-ts-interval = "200ms"`. Start with the `nightly` tags.
- [ ] **Step 2:** In `spike/`, generate `cdcpb` bindings from `pingcap/kvproto` with `tonic-build`, write keys under `k/` with unpatched `tikv-client` (optimistic, `try_one_pc`), and subscribe to `EventFeed` with `kv_api = TiDB` for the `k/` range.
- [ ] **Step 3:** Answer by observation, each with the command output that shows it:
  1. Do events arrive for those keys? Which row types for a one-phase commit and for a two-key transaction?
  2. Are `start_key`/`end_key` in the request and `key` in events raw or memcomparable-encoded?
  3. Does `ResolvedTs` advance about every 200ms on an idle cluster? Does it advance for an idle region?
  4. Does a raw `kvrpcpb.Get` with `need_commit_ts = true` return a non-zero `commit_ts` equal to the timestamp `commit()` returned? If not on `nightly`, the feature is unreleased: stop.
  5. Find the oldest released TiKV tag for which 4 holds and pin both images to it.
- [ ] **Step 4:** Replace the from-reading claims in the spec's Facts section with these observed results (including the key encoding and the pinned version). Delete `spike/`.
- [ ] **Step 5: Gate.** If 1, 3 or 4 failed, stop and return to the design with the evidence. Otherwise commit `dev/docker-compose.yml` and the spec change.

### Task 1: Vendored `tikv-client` patch

**Files:**
- Create: `vendor/tikv-client/` (copy of `tikv/client-rust` at the commit used in the spike, `.git` removed)
- Modify: `vendor/tikv-client/src/lib.rs`, `src/transaction/transaction.rs`, `src/transaction/snapshot.rs`, `src/transaction/client.rs`
- Modify: `Cargo.toml` (`tikv-client = { path = "vendor/tikv-client" }`)
- Test: `tests/it_client_patch.rs`

**Interfaces — Produces:**
```rust
pub mod proto;                                   // was private
impl Transaction { pub async fn get_with_commit_ts(&mut self, key: impl Into<Key>) -> Result<Option<(Value, Timestamp)>>;
                   pub async fn batch_get_with_commit_ts(&mut self, keys: impl IntoIterator<Item = impl Into<Key>>) -> Result<Vec<(KvPair, Timestamp)>>; }
impl Snapshot    { /* the same two, delegating */ }
pub struct RegionRoute { pub region: proto::metapb::Region, pub leader: proto::metapb::Peer, pub store_address: String }
impl TransactionClient { pub async fn regions_in_range(&self, range: impl Into<BoundRange>) -> Result<Vec<RegionRoute>>;
                         pub async fn cluster_id(&self) -> u64; }
```
Both reads bypass the transaction's write buffer and read at its start timestamp.

- [ ] **Step 1:** Write `tests/it_client_patch.rs`:
  - `commit_ts_round_trips`: put `k/a`, `let c = txn.commit().await?.unwrap()`; then on a fresh snapshot `get_with_commit_ts("k/a")` returns `Some((value, ts))` with `ts.version() == c.version()`.
  - `batch_returns_each_keys_own_commit_ts`: two keys written in two transactions return their two distinct commit timestamps.
  - `missing_key_is_none`.
  - `regions_cover_range`: `regions_in_range("k/".."k0")` is non-empty, sorted by start key, contiguous, and every `store_address` is non-empty.
- [ ] **Step 2:** Run `TIKVD_TEST_PD=127.0.0.1:2379 cargo test --test it_client_patch`. Expected: fails to compile (methods missing).
- [ ] **Step 3:** Implement the patch. Set `need_commit_ts = true` on the lowered `GetRequest`/`BatchGetRequest` and read `commit_ts` from the response and from each `KvPair`.
- [ ] **Step 4:** Run the same command. Expected: 4 passed.
- [ ] **Step 5:** Commit.

### Task 2: Keys and records

**Files:**
- Create: `src/lib.rs`, `src/keys.rs`, `src/record.rs`

**Interfaces — Produces:**
```rust
// keys.rs
pub fn data_key(etcd_key: &[u8]) -> Vec<u8>;                         // b"k/" + key
pub fn etcd_key(data_key: &[u8]) -> Option<&[u8]>;
pub fn data_range(key: &[u8], range_end: &[u8]) -> (Vec<u8>, Vec<u8>); // half-open TiKV range
pub fn ttl_key(expiry_unix_secs: u64, etcd_key: &[u8]) -> Vec<u8>;   // b"l/" + expiry big-endian + key
pub fn parse_ttl_key(ttl_key: &[u8]) -> Option<(u64, &[u8])>;
pub fn ttl_range_until(now_unix_secs: u64) -> (Vec<u8>, Vec<u8>);
// record.rs
pub struct Record { pub version: i64, pub created: u64, pub lease_expiry: u64, pub value: Vec<u8> }
pub struct CorruptRecord;
impl Record { pub fn encode(&self) -> Vec<u8>; pub fn decode(bytes: &[u8]) -> Result<Record, CorruptRecord>; }
```
`data_range` follows etcd: empty `range_end` means the single key (`[k, k + 0x00)`); `range_end == [0]` means to the end of the data prefix (`b"k0"`); otherwise `[data_key(key), data_key(range_end))`. Record layout is three fixed-width big-endian integers followed by the value.

- [ ] **Step 1:** Unit tests in each file:
  - `data_range(b"a", b"")` contains `data_key(b"a")` and not `data_key(b"a\0")`.
  - `data_range(b"a", b"\0").1 == b"k0"`.
  - `data_range(b"a\xff", b"b")` contains `data_key(b"a\xff\xff")`.
  - `etcd_key(&data_key(k)) == Some(k)`; `etcd_key(b"l/x") == None`.
  - `parse_ttl_key(&ttl_key(7, b"a/b")) == Some((7, b"a/b"))`; `ttl_key(1, _) < ttl_key(2, _)`; `ttl_range_until(5)` contains `ttl_key(5, b"x")` and not `ttl_key(6, b"")`.
  - `Record::decode(&r.encode()) == r` for an empty value and a 1 MiB value; `Record::decode(&[1, 2, 3])` is `Err(CorruptRecord)`.
- [ ] **Step 2:** `cargo test --lib`. Expected: fails to compile.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** `cargo test --lib`. Expected: all pass.
- [ ] **Step 5:** Commit.

### Task 3: Store (reads, writes, compaction over TiKV)

**Files:**
- Create: `src/store.rs`
- Test: `tests/it_store.rs`

**Interfaces — Consumes:** Task 1 and Task 2. **Produces:**
```rust
pub struct KeyValue { pub key: Vec<u8>, pub value: Vec<u8>, pub mod_revision: i64, pub create_revision: i64, pub version: i64, pub lease: i64 }
pub struct RangeQuery { pub key: Vec<u8>, pub range_end: Vec<u8>, pub limit: i64, pub revision: i64, pub keys_only: bool, pub count_only: bool }
pub struct RangeResult { pub kvs: Vec<KeyValue>, pub more: bool, pub count: i64, pub revision: i64 }
pub enum Compare { ModRevision(i64), Version(i64) }
pub enum Write { Put { value: Vec<u8>, ttl_secs: Option<i64> }, Delete }
pub enum WriteOutcome { Applied { revision: i64, prev: Option<KeyValue> }, Rejected { revision: i64, current: Option<KeyValue> } }
pub enum StoreError { Compacted, Corrupt(Vec<u8>), Tikv(tikv_client::Error) }
impl Store {
    pub async fn connect(pd_endpoints: Vec<String>) -> Result<Store, StoreError>;
    pub fn client(&self) -> &tikv_client::TransactionClient;
    pub async fn now(&self) -> Result<i64, StoreError>;
    pub async fn range(&self, q: RangeQuery) -> Result<RangeResult, StoreError>;
    pub async fn write(&self, key: &[u8], compare: Option<Compare>, write: Write) -> Result<WriteOutcome, StoreError>;
    pub async fn delete_range(&self, key: &[u8], range_end: &[u8]) -> Result<(i64, i64), StoreError>; // (deleted, revision)
    pub async fn compact(&self, revision: i64) -> Result<(), StoreError>;
}
```
Decisions: `revision == 0` in a query means "fetch a timestamp now". `create_revision` is `mod_revision` when `version == 1`, else `created`. `lease` is the TTL in seconds originally granted, or 0. A missing key compares as `ModRevision(0)` and `Version(0)`. `Delete` on a missing key is `Applied` with `prev: None` and the transaction's start timestamp as revision. `write` uses an optimistic two-phase transaction (never `try_one_pc`: its commit timestamps are not unique), and on a TiKV write conflict retries from the read with no cap other than the request's deadline. `Rejected.revision` is the read timestamp. `compact` calls the client's `gc` with the revision as safepoint; a snapshot read that TiKV rejects as below the safepoint maps to `StoreError::Compacted`.

- [ ] **Step 1:** Write `tests/it_store.rs`; each test uses a unique random key prefix:
  - `create_then_get`: `write(k, Some(ModRevision(0)), Put)` is `Applied { revision: r, prev: None }`; `range` of `k` returns one kv with `mod_revision == r`, `create_revision == r`, `version == 1`.
  - `update_bumps_version_and_keeps_create_below_mod`: after a second put, `version == 2` and `create_revision < mod_revision`.
  - `stale_compare_is_rejected_with_current`: `write(k, Some(ModRevision(r - 1)), Put)` is `Rejected { current: Some(kv), .. }` with `kv.mod_revision == r`.
  - `version_compare`: `Some(Version(1))` applies after one put, `Some(Version(5))` is rejected.
  - `concurrent_creates_one_winner` (Review Focus 1): 20 tasks call `write(k, Some(ModRevision(0)), Put)`; exactly 1 `Applied`, 19 `Rejected` whose `current` has the winner's value.
  - `read_at_old_revision`: put v1 (r1), put v2 (r2); `range` with `revision: r1` returns v1 with `mod_revision == r1`.
  - `paging`: 5 keys, `limit: 2` returns 2 kvs, `more == true`, `count == 5`; continuing from the next key at the same `revision` returns the rest, and a key written in between is not seen.
  - `edges` (Review Focus 5): `range_end = b"\0"` returns every key at or after `key`; a prefix ending in `0xff` lists its children; an empty value round-trips.
  - `keys_only` returns empty values; `count_only` returns no kvs and the right `count`.
  - `delete_then_recreate_resets_version`: `version == 1` and `create_revision == mod_revision` after recreate.
  - `put_with_ttl_writes_index`: after `Put { ttl_secs: Some(60) }`, a TiKV scan of `ttl_range_until(now + 61)` contains the key; after a second put with no TTL, it does not.
- [ ] **Step 2:** `TIKVD_TEST_PD=127.0.0.1:2379 cargo test --test it_store`. Expected: fails to compile.
- [ ] **Step 3:** Implement `Store`. Range reads are `scan` for keys then `batch_get_with_commit_ts`; `more` comes from scanning `limit + 1`; `count` from `scan_keys` over the whole range (`// ponytail: exact count rescans keys per page, estimate if lists get slow`).
- [ ] **Step 4:** Run the same command. Expected: all pass.
- [ ] **Step 5:** Commit.

### Task 4: etcd protos, KV service, binary

**Files:**
- Create: `proto/etcd/rpc.proto`, `proto/etcd/kv.proto` (from `etcd-io/etcd` `api/`, with gogoproto and google.api options and the Auth, Cluster services removed; field numbers untouched), `build.rs`
- Create: `src/kv_service.rs`, `src/server.rs`
- Modify: `src/main.rs`, `Cargo.toml`
- Test: `tests/common/mod.rs`, `tests/it_kv.rs`

**Interfaces — Consumes:** `Store`. **Produces:**
```rust
pub struct Config { pub listen: SocketAddr, pub pd_endpoints: Vec<String>, pub tls: Option<(PathBuf, PathBuf)> }
pub async fn serve(config: Config, shutdown: impl Future<Output = ()>) -> anyhow::Result<()>;   // server.rs
// tests/common/mod.rs
pub async fn start_tikvd() -> (etcd_client::Client, SocketAddr);   // serves on 127.0.0.1:0
```
Flags: `--listen` (default `127.0.0.1:2379` is taken by PD in dev, so default `0.0.0.0:2479`), `--pd` (repeatable), `--cert`, `--key`. Error mapping: `StoreError::Compacted` → gRPC `OUT_OF_RANGE` with message `etcdserver: mvcc: required revision has been compacted`; `Tikv` → `UNAVAILABLE`; `Corrupt` → `DATA_LOSS`. Every response header carries `revision`; `cluster_id`, `member_id` and `raft_term` are fixed non-zero constants.

Supported `Txn` shapes are exactly the spec's; the nested `ResponsePut`/`ResponseDeleteRange` carries its own header with the new revision, and the `else` branch's `ResponseRange` carries the current kv. Every request is logged at debug level with its shape, and an unsupported one is logged at warn before returning `UNIMPLEMENTED`.

- [ ] **Step 1:** Write `tests/it_kv.rs` using `etcd-client`:
  - `put_get_delete`: put, get returns value and `mod_revision == put header revision`; delete; get returns nothing.
  - `kube_create_txn`: `Txn.when([mod_revision(k) == 0]).and_then([put]).or_else([get])` succeeds first time; the second attempt has `succeeded == false` and the else-branch range response holds the existing kv.
  - `kube_update_txn_reports_new_revision`: the nested put response's header revision equals a following get's `mod_revision`.
  - `compactor_version_txn`: compare `version(k) == 0` then put succeeds; repeating it fails and returns the kv.
  - `unsupported_txn_is_unimplemented`: a two-key transaction returns status code `Unimplemented`.
  - `prefix_list_with_limit`: `more` and `count` as in Task 3.
- [ ] **Step 2:** `TIKVD_TEST_PD=127.0.0.1:2379 cargo test --test it_kv`. Expected: fails to compile.
- [ ] **Step 3:** Add protos and `build.rs`; implement `KvService`, `serve`, `main`.
- [ ] **Step 4:** Run the same command. Expected: all pass.
- [ ] **Step 5:** Commit.

### Task 5: Event assembly and sorting (pure logic)

**Files:**
- Create: `src/cdc/mod.rs`, `src/cdc/assemble.rs`, `src/cdc/sorter.rs`

**Interfaces — Consumes:** `tikv_client::proto::cdcpb`, `Record`, `keys`. **Produces:**
```rust
pub struct ChangeEvent { pub key: Vec<u8>, pub revision: i64, pub current: Option<Record>, pub previous: Option<Record> }  // current None = delete, previous None = create
impl Assembler { pub fn new() -> Self; pub fn on_row(&mut self, row: cdcpb::event::Row) -> Result<Option<ChangeEvent>, CorruptRecord>; pub fn forget_region_prewrites(&mut self, keys_in: &(Vec<u8>, Vec<u8>)); }
impl Sorter {
    pub fn new(memory_limit_bytes: usize) -> Self;
    pub fn set_regions(&mut self, region_ids: &[u64]);              // regions that must all be resolved
    pub fn set_resolved(&mut self, region_id: u64, ts: u64);
    pub fn push(&mut self, event: ChangeEvent) -> Result<(), SorterFull>;
    pub fn drain(&mut self) -> (Vec<ChangeEvent>, i64);            // events <= watermark in order, and the watermark
}
```
Row handling: `COMMITTED` → event now; `PREWRITE` → held by `(key, start_ts)`; `COMMIT` → joins its held prewrite; `ROLLBACK` → drops it; `INITIALIZED` and unknown types → `None`. Rows whose key is not under `k/` are ignored. Key decoding uses the encoding recorded in the spec by Task 0. The watermark is the minimum resolved timestamp across the set regions, and never decreases.

- [ ] **Step 1:** Unit tests:
  - `committed_row_is_an_event`; `prewrite_then_commit_joins` (value from prewrite, revision from commit); `rollback_drops_prewrite`; `commit_without_prewrite_is_none`.
  - `no_old_value_is_create`; `delete_row_has_no_current_and_has_previous`.
  - `drain_orders_by_revision_across_regions`: push revisions 30, 10, 20, resolve both regions to 25 → drains `[10, 20]`, watermark 25; 30 stays.
  - `unresolved_region_holds_watermark` (Review Focus 3): regions {1, 2}, region 1 resolved to 100, region 2 never → drains nothing, watermark 0. Then region 2 resolves to 50 and region 1 moves to 200 → watermark 50.
  - `replayed_event_is_deduplicated`: same `(key, revision)` pushed twice drains once.
  - `event_at_or_below_watermark_after_reregister_is_dropped`: pushing revision 5 when the watermark is already 10 does not emit it.
  - `over_limit_is_sorter_full`.
- [ ] **Step 2:** `cargo test --lib cdc`. Expected: fails to compile.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** `cargo test --lib cdc`. Expected: all pass.
- [ ] **Step 5:** Commit.

### Task 6: CDC feed (regions, streams, lock resolution)

**Files:**
- Create: `src/cdc/feed.rs`
- Test: `tests/it_feed.rs`

**Interfaces — Consumes:** Task 1 (`regions_in_range`, `cluster_id`, `proto::cdcpb`, `cleanup_locks`), Task 5. **Produces:**
```rust
pub enum FeedOutput { Events { events: Vec<ChangeEvent>, watermark: i64 }, Reset }
pub struct FeedConfig { pub stalled_lock_threshold: Duration, pub unhealthy_after: Duration, pub sorter_memory_limit: usize }  // defaults 5s, 30s, 256 MiB
pub fn spawn_feed(store: Store, config: FeedConfig) -> (tokio::sync::mpsc::Receiver<FeedOutput>, FeedHealth);
impl FeedHealth { pub fn is_serving(&self) -> bool; }   // false until the first watermark, and when stalled past unhealthy_after
```
Behaviour, per the spec's Watch section steps 1–6: one `EventFeed` stream per store; register each region with `extra_op = ReadOldValue`, `kv_api = TiDB`, its epoch, and `checkpoint_ts`; on a region error re-resolve that key range through `regions_in_range` and re-register from the range's last resolved timestamp (a region with no live registration simply stops reporting, which holds the watermark); when a region lags the newest resolved timestamp by more than `stalled_lock_threshold`, call the client's lock cleanup for that region's range with a safepoint of `now - stalled_lock_threshold`. `FeedOutput::Reset` is sent, and the feed restarts from a fresh timestamp, on `SorterFull` or when TiKV rejects a checkpoint as older than GC. The feed sends an `Events` output with an empty vector when only the watermark moved.

- [ ] **Step 1:** Write `tests/it_feed.rs`:
  - `sees_writes_in_commit_order`: write 50 keys from 5 concurrent tasks through `Store`; collected events have strictly increasing `revision`, and the multiset of `(key, revision)` equals what the `write` calls returned.
  - `watermark_advances_when_idle`: with no writes, two `Events` outputs arrive within 2s with increasing watermark.
  - `create_update_delete_shapes`: create has `previous == None`; update has both; delete has `current == None` and `previous` equal to the last value.
  - `abandoned_lock_does_not_stall_forever`: leave a prewrite lock behind on a key under `k/` by sending a raw `kvrpcpb.Prewrite` (through the vendored client's public `proto` and request plan) and never committing it. First assert the premise with `stalled_lock_threshold = 1h`: the watermark stays below the lock's start timestamp for 5s. Then with `stalled_lock_threshold = 1s`: the watermark passes it within 10s.
  - `serving_flips_true_after_first_watermark`.
- [ ] **Step 2:** `TIKVD_TEST_PD=127.0.0.1:2379 cargo test --test it_feed`. Expected: fails to compile.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run the same command. Expected: all pass.
- [ ] **Step 5:** Commit.

### Task 7: Watch hub and Watch service

**Files:**
- Create: `src/watch_hub.rs`, `src/watch_service.rs`
- Modify: `src/server.rs`
- Test: unit tests in `src/watch_hub.rs`, `tests/it_watch.rs`

**Interfaces — Consumes:** `ChangeEvent`, `FeedOutput`, `Store::now`. **Produces:**
```rust
impl WatchHub {
    pub fn new(window_events: usize, subscriber_buffer: usize) -> Arc<WatchHub>;   // defaults 100_000 and 1024 batches
    pub fn apply(&self, output: FeedOutput);
    pub fn watermark(&self) -> i64;
    pub fn subscribe(&self, key: Vec<u8>, range_end: Vec<u8>, after_revision: i64) -> Result<Subscription, Compacted>;
}
pub enum Delivery { Events { events: Vec<ChangeEvent>, watermark: i64 }, Progress { watermark: i64 }, Cancelled { compacted: bool } }
impl Subscription { pub async fn next(&mut self) -> Delivery; }
```
Decisions: `subscribe` delivers events with `revision > after_revision`. It returns `Err(Compacted)` when `after_revision` is below the oldest revision the window can vouch for (the instance's first watermark, then the revision before the oldest retained event). The service turns a request's `start_revision` into `after_revision = start_revision - 1`, or `Store::now()` when it is 0. A subscriber whose buffer is full is sent `Cancelled { compacted: false }` and removed. `FeedOutput::Reset` sends `Cancelled { compacted: true }` to everyone and empties the window. The service maps `Cancelled { compacted: true }` to a `WatchResponse` with `canceled = true` and `compact_revision` set to the hub's oldest revision. Watch events set `kv.create_revision == kv.mod_revision` exactly when `previous` is `None`; `prev_kv` is filled when requested, with `mod_revision` 0 because TiKV's old value carries no commit timestamp. `WatchProgressRequest` and `progress_notify` (every 5s when idle) send an empty response whose header revision is the watermark.

- [ ] **Step 1:** Unit tests in `watch_hub.rs`:
  - `replays_window_then_follows_live`.
  - `future_revision_waits` (Review Focus 2): watermark 10; `subscribe(.., after_revision: 20)`; apply events 15, 20, 25 with watermark 30 → the subscriber receives only 25.
  - `range_filter`: a subscriber on prefix `a/` does not receive `b/x`.
  - `too_old_is_compacted`.
  - `slow_subscriber_is_cancelled_and_others_continue` (Review Focus 4): buffer of 2, one subscriber never reads, another does; after 10 applies the first gets `Cancelled { compacted: false }` and the second received all 10.
  - `reset_cancels_everyone_as_compacted`.
- [ ] **Step 2:** Write `tests/it_watch.rs` with `etcd-client`:
  - `list_then_watch_has_no_gap_or_overlap`: writer task puts 200 keys; meanwhile list a prefix at revision `r`, watch from `r + 1`; list ∪ watch equals the 200 keys with no key seen in both at the same revision.
  - `create_vs_modify`: first event has `create_revision == mod_revision`, second does not.
  - `delete_event_carries_prev_kv`.
  - `progress_request_returns_watermark_header`: revision is positive and non-decreasing across two requests.
  - `watch_from_ancient_revision_is_cancelled_compacted`: `start_revision = 1`.
- [ ] **Step 3:** `cargo test --lib watch_hub` and `TIKVD_TEST_PD=127.0.0.1:2379 cargo test --test it_watch`. Expected: fail to compile.
- [ ] **Step 4:** Implement the hub, the service, and wire `spawn_feed` → `WatchHub::apply` in `serve`.
- [ ] **Step 5:** Run both commands. Expected: all pass.
- [ ] **Step 6:** Commit.

### Task 8: Leases and the reaper

**Files:**
- Create: `src/lease.rs`
- Modify: `src/kv_service.rs` (put with lease), `src/server.rs`
- Test: `tests/it_lease.rs`

**Interfaces — Consumes:** `Store`, `keys::ttl_range_until`. **Produces:**
```rust
pub fn spawn_reaper(store: Store, interval: Duration);     // default 1s
impl Store { pub async fn delete_if_expired(&self, key: &[u8], expiry_unix_secs: u64) -> Result<bool, StoreError>; }
```
`LeaseGrant { ttl }` returns `id = ttl` and the same `ttl`, storing nothing; a client-supplied non-zero `id` is rejected as `InvalidArgument`. A put with `lease = id` is `Write::Put { ttl_secs: Some(id) }`. `delete_if_expired` deletes the data key and the index key in one transaction only when the record's `lease_expiry` still equals `expiry_unix_secs`; otherwise it deletes just the stale index key. The reaper scans at most 1000 index keys per tick.

- [ ] **Step 1:** Write `tests/it_lease.rs`:
  - `key_with_ttl_disappears_and_watchers_see_delete`: grant 1s, put with it, watch the key; within 5s the watch gets a delete event and a get returns nothing.
  - `rewrite_without_lease_survives`: put with 1s lease, immediately put again with no lease; after 3s the key still exists.
  - `two_reapers_do_not_double_delete`: two `tikvd` instances; exactly one delete event is observed.
  - `kv_reports_lease`: a get shows `lease == ttl`.
- [ ] **Step 2:** `TIKVD_TEST_PD=127.0.0.1:2379 cargo test --test it_lease`. Expected: fails to compile.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run the same command. Expected: all pass.
- [ ] **Step 5:** Commit.

### Task 9: Compaction, Maintenance, health, TLS

**Files:**
- Create: `src/maintenance.rs`
- Modify: `src/kv_service.rs` (`Compact`), `src/server.rs` (gRPC health, TLS), `Cargo.toml` (`tonic-health`)
- Test: `tests/it_ops.rs`

**Interfaces — Consumes:** `Store::compact`, `WatchHub::watermark`, `FeedHealth`.

`Status` returns `version = "3.5.13"`, header revision = watermark, `leader` = the fixed member id, `db_size = 0`. gRPC health for the empty service name and for `etcdserverpb.Maintenance` follows `FeedHealth::is_serving`. `Compact` with a revision above `Store::now()` is `OUT_OF_RANGE` with `etcdserver: mvcc: required revision is a future revision`.

- [ ] **Step 1:** Write `tests/it_ops.rs`:
  - `status_reports_version`: `"3.5.13"`.
  - `compact_then_old_read_is_compacted`: put v1 (r1), put v2 (r2), compact r2; a get at `revision = r1` fails with a message containing `required revision has been compacted`. (TiKV GC is asynchronous; poll for up to 60s.)
  - `compact_future_revision_rejected`.
  - `health_serving_after_start`.
  - `tls_round_trip`: start with a self-signed certificate generated in the test (`rcgen` dev-dependency), connect with that CA, put and get.
- [ ] **Step 2:** `TIKVD_TEST_PD=127.0.0.1:2379 cargo test --test it_ops`. Expected: fails to compile.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run the same command. Expected: all pass.
- [ ] **Step 5:** Commit.

### Task 10: k3s and Argo Workflows acceptance

**Files:**
- Create: `dev/acceptance.sh`
- Modify: `dev/docker-compose.yml` (add a `k3s` profile using the current stable `rancher/k3s` image, privileged, host networking)
- Modify: the spec's Assumptions section (replace "from memory" with the observed request shapes)

- [ ] **Step 1:** Write `dev/acceptance.sh`: start PD and TiKV; start two `tikvd` release builds on `:2479` and `:2480` with debug request logging to files; start k3s with `--datastore-endpoint=http://127.0.0.1:2479,http://127.0.0.1:2480`; wait for `kubectl get nodes` to show `Ready`; `kubectl create namespace argo`; apply the current Argo Workflows `quick-start-minimal.yaml`; wait for the `workflow-controller` deployment to be available; submit the upstream `hello-world` workflow with `kubectl create`; wait up to 5 minutes for `status.phase == Succeeded`; then `grep -c UNIMPLEMENTED` both logs. Exit non-zero on any failure or any `UNIMPLEMENTED` line, printing those lines.
- [ ] **Step 2:** Run `dev/acceptance.sh`. For each `UNIMPLEMENTED` or failure, decide whether kube-apiserver needs it; if so add a failing test to the owning task's test file, implement, and rerun. Do not widen support for shapes nothing sent.
- [ ] **Step 3:** Kill one `tikvd` while a second workflow runs; it must still reach `Succeeded`.
- [ ] **Step 4:** Record the observed request shapes and the pinned k3s version in the spec's Assumptions section.
- [ ] **Step 5:** Run `dev/acceptance.sh` once more from a clean cluster. Expected: exit 0, final line `workflow Succeeded`. Commit.
