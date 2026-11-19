# tikvd: an etcd v3 front-end for TiKV

Status: draft for approval. 2026-11-19.

## Intent

Run Kubernetes with TiKV as its datastore instead of etcd. `tikvd` is a
stateless Rust server that speaks the part of etcd's v3 gRPC API that
kube-apiserver uses, and keeps all state in a TiKV cluster.

The reason for building it, rather than using something that exists, is
scale: any number of `tikvd` instances must be able to serve the same
cluster, and write throughput must not be capped by a single serialised
sequence the way etcd, Kine, or a counter-key design are.

## Goals

- kube-apiserver runs unmodified against one or more `tikvd` endpoints.
- Any number of `tikvd` instances serve the same TiKV cluster at once, with
  no coordination between them and no instance-local durable state.
- Writes to different keys do not contend with each other.
- Acceptance test: a k3s server started with `--datastore-endpoint` pointing
  at two `tikvd` instances comes up healthy, Argo Workflows installs on it,
  and a hello-world workflow runs to `Succeeded`.

## Non-goals

- Full etcd compatibility. Only what kube-apiserver sends is supported;
  everything else returns `UNIMPLEMENTED`. `etcdctl get/put/del/watch` on
  single keys and prefixes should work as a side effect, for debugging.
- etcd's Auth, Cluster (member add/remove), Lock and Election services.
- Exact `create_revision`. Only whether it equals `mod_revision` is kept
  exact, because kube-apiserver uses that to tell a create from an update.
- Sharing the TiKV cluster with other users. See Assumptions.
- Dense revisions. Revisions increase but have large gaps.
- Running or managing TiKV and PD. They are assumed to exist.

## Anti-goals

- A lost or reordered watch event. Kubernetes controllers silently diverge
  when this happens, so correctness of watch beats latency and throughput
  everywhere they conflict.
- Any design where correctness depends on there being one `tikvd`.

## Approaches considered

**A. Counter key.** Every write bumps one revision counter in the same TiKV
transaction and appends to a revision-ordered log; watch tails the log.
Simple and correct, but all writes contend on one key. Rejected: it caps
write throughput, which is the reason this project exists.

**B. TiKV commit timestamps as revisions, watch fed by TiKV CDC.** No shared
write key. Costs: a hand-written CDC client, and watch delivery that trails
real time by TiKV's resolved-timestamp interval. **Chosen** by the user for
write scalability.

**C. Kine on TiDB.** No code, but the same single-sequence bottleneck as A
plus a SQL layer. Rejected for the same reason as A.

## Facts this design rests on

Observed on 2026-11-19 against one PD and one TiKV in Docker
(`dev/docker-compose.yml`, then with `cdc.min-ts-interval = "200ms"`; fact 9
at `"10ms"`), on the pinned
v8.5.6 and on nightly (9.0.0-beta.2), with unpatched `tikv-client` 0.4.0
writing and gRPC bindings generated from `pingcap/kvproto` master reading.
Items marked "from source" were read in `tikv/tikv` and `tikv/client-rust`
but not observed.

1. CDC is a bidirectional gRPC stream per TiKV store
   (`cdcpb.ChangeData/EventFeed`). The client registers each region
   separately with its epoch, a key range and a `checkpoint_ts`, and gets
   one `INITIALIZED` row per registration when its incremental scan ends.
   Keys written under `k/` by `tikv-client` (optimistic, `try_one_pc`)
   arrive as rows:
   - One-phase commit, one key or two keys in one region: one `COMMITTED`
     row per key, carrying `start_ts`, `commit_ts`, value and old value.
   - Two-phase commit of two keys: one `PREWRITE` row per key (value, old
     value, `commit_ts` 0), then one `COMMIT` row per key (`commit_ts` and
     value, old value empty). The old value is only on the `PREWRITE` row.
   - One-phase delete: one `COMMITTED` row, op type `DELETE`, empty value,
     old value set.
   - Two-phase delete of two keys: one `PREWRITE` row per key (op type
     `DELETE`, empty value, old value set, `commit_ts` 0), then one
     `COMMIT` row per key (op type `DELETE`, `commit_ts`, empty value, old
     value empty).
   - Incremental-scan rows are `COMMITTED` with old values, ordered by key
     and newest first within a key, not by commit timestamp.
2. Key encoding. `start_key` and `end_key` in `ChangeDataRequest` must be
   memcomparable-encoded (TiKV's bytes codec: 8-byte groups padded with
   `0x00`, each followed by `0xFF` minus the pad count). `[k/, k0)` is sent
   as `[6B2F000000000000F9, 6B30000000000000F9)`. Raw bounds are not
   rejected to the client: TiKV logs "cdc invalid observed start key or end
   key version" and silently observes the whole region, so a key outside
   the range (`j/outside`) was delivered. An empty `end_key` takes the same
   path and also means the whole region. The `key` in event rows is raw
   (`k/one` arrives as `6B2F6F6E65`). PD region bounds are memcomparable
   too.
3. Transactional change events are sent to subscriptions that use
   `kv_api = TiDB`. `TxnKV` on this cluster (`storage.api-version = 1` in
   TiKV's `/config`) gets a per-region
   `Error` with `compatibility.required_version = "6.2.0"` and no rows.
4. With `cdc.min-ts-interval = "200ms"`, `ResolvedTs` advances every
   200ms on an idle cluster: over a 35s run on v8.5.6 (writes only in
   0.1s of it), 172 messages, mean step
   202.6ms of physical time (min 199, max 251). It advances equally for a
   registered region that has never had a write. It arrives as one
   store-level `ChangeDataEvent.resolved_ts` per tick listing all
   registered regions, with `request_id` 0, when `header.ticdc_version` is
   8.5.0 or 4.0.8. With 4.0.7 or an empty string it arrives instead as a
   per-region `Event::ResolvedTs` carrying the registration's
   `request_id`, at the same cadence. The stream-multiplexing header was
   not tried. From source: the default interval is 1s.
5. A raw `kvrpcpb.Get` with `need_commit_ts = true` returns a `commit_ts`
   equal to the timestamp `Transaction::commit` returned, for one-phase
   and two-phase commits, on v8.5.6, v8.5.8 and nightly. Without the flag
   it returns 0; for a deleted key it returns `not_found` and 0. A read of
   a two-phase secondary key sent right after `commit` returned once got a
   `KeyError` with that transaction's lock instead. Tags tested: v7.5.8,
   v8.1.0, v8.5.0, v8.5.4, v8.5.5, v8.5.6, v8.5.8, v9.0.0-beta.1 and
   nightly. It holds on v8.5.6, v8.5.8 and nightly and returns 0 on the
   rest. Both images are pinned to v8.5.6, the oldest tag tested on which
   it holds. Other tags, including v8.5.1 to v8.5.3, v8.5.7 and
   everything before v7.5.8, were not tested.
   From source: `BatchGet` accepts the flag too and `Scan` does not.
6. `Transaction::commit` in `tikv-client` 0.4.0 returns `Some` commit
   timestamp for one-phase and two-phase commits. From source: it does not
   set `need_commit_ts`, does not expose CDC, and keeps its protobuf types
   and PD/region client private. Its default `prometheus` feature needs a
   system OpenSSL to build.
7. From source: TiKV does not publish a resolved timestamp for a region
   whose range has shrunk (by a split or merge) before it sends that
   region's error to its CDC subscribers. The feed's safety relies on
   this, because it applies a resolved timestamp to the whole range the
   registration was made for.
8. Observed by the final review's probe on v8.5.6: an abandoned prewrite
   lock on an `l/` key in the same region as `k/` keys did not hold back
   the resolved timestamp seen by a subscription bounded to `k/`. This is
   why lock cleanup scoped to the `k/` range is enough. On a TiKV version
   that tracked locks region-wide, such a lock would instead stall every
   instance's watermark until something else resolved it.
9. Observed with `cdc.min-ts-interval = "10ms"` on the one-store dev
   cluster: one writer putting a key every 10 ms through an in-process
   `tikvd` that watches the prefix, 600 writes per run, two runs each, time
   from the put's response to the watch event. With the feed releasing on a
   100 ms tick the lag was 56 ms p50 and 106 to 107 ms p99. Releasing from
   the CDC message that moves the watermark, it was 5.6 to 5.7 ms p50 and
   11.3 ms p99, max 11.8 ms. No event was missing in either.

## Assumptions

These are the builder's calls, not decisions the user made.

- The TiKV cluster is dedicated to `tikvd` and runs API V1. Compaction
  moves the cluster-wide GC safepoint, which would destroy history for any
  other tenant.
- The target is k3s v1.36.2+k3s1 (`rancher/k3s:v1.36.2-k3s1`, etcd client
  v3.6.12-k3s1), run by `dev/acceptance.sh` against two `tikvd` instances
  with Argo Workflows v4.1.6 installed. The request shapes below were
  observed on 2026-11-19 by logging every request it sent; every one was
  served and none returned `UNIMPLEMENTED`.
  - `Range`: a single key with `limit` 0 or 1; a key range with `limit` 0
    or 10000 at `revision` 0, or with `limit` 10000 at an explicit
    revision (the cache consistency checker, whose CRD list is about 6 MB);
    a key range with `count_only`. `keys_only`, sort and the revision
    filters were never sent.
  - `Txn`, always one `Equal` compare on one key, then one op on that key:
    `mod_revision = 0` then `Put` (with or without a lease), with an empty
    else or an else `Range` of that key; `mod_revision = R` then `Put`
    (with or without a lease) else `Range`; `mod_revision = R` then
    `DeleteRange` else `Range`; `version = N` on `compact_rev_key` then
    `Put` else `Range` (the compactor). The else `Range` never carried
    options. `prev_kv`, `ignore_value` and `ignore_lease` were never set.
  - Bare `Put` and bare `DeleteRange` were never sent.
  - `Watch` create: key ranges with `prev_kv` and `progress_notify`, and
    one single-key watch on `compact_rev_key` with neither; always a
    positive `start_revision`, no filters, no client watch IDs. Watch
    progress requests are sent continually.
  - `LeaseGrant` with TTL 15 and 3660 and no ID. No other lease call.
  - `Compact` every five minutes after the first compactor round.
  - `Maintenance.Status`. No other maintenance call.

## Design

### Revisions

An etcd revision is a TiKV commit timestamp. This is forced, not chosen:
the resolved timestamp is a promise about commit timestamps only. A
revision taken from anything else (a transaction's start timestamp, or a
timestamp fetched mid-transaction and stored in the value) can commit after
the watermark has already passed it, which would deliver a watch event out
of order.

Consequences:

- A write's response revision is the timestamp returned by `commit`.
- A revision identifies exactly one transaction. Writes therefore use
  ordinary two-phase commit, whose commit timestamp comes from PD's
  timestamp oracle and is never issued twice. One-phase and async commit
  must not be used: TiKV computes their commit timestamp locally, and two
  transactions on different keys can share it (observed: 50 concurrent
  writes produced 23 distinct timestamps). A watcher resuming at the next
  revision would then lose an event.
- Several events share a revision only when they come from one transaction
  (a range delete). They are delivered to a watcher in one response.
- A key's `mod_revision` is the commit timestamp of its latest version,
  read back with `need_commit_ts`. It is never stored in the value, because
  it is not known until after commit.
- A read at revision R is a TiKV snapshot read at timestamp R.
- A read "now" fetches a fresh timestamp T from PD, reads a snapshot at T,
  and reports T as the header revision. TiKV snapshot isolation guarantees
  that every commit at or below T is in the result and none will appear
  later, so "list at T, then watch from T+1" has no gap and no overlap.

### Data layout

All keys live under one fixed prefix so the CDC subscription and scans are
bounded.

- `k/<etcd key>` → record: `version` (count of writes since creation),
  `created` (start timestamp of the creating transaction), `lease` (expiry
  time in unix seconds, or 0), then the value bytes.

`create_revision` is reported as `mod_revision` when `version` is 1, and as
`created` otherwise. `created` is always lower than any later commit, so
the two are equal exactly on a create.
- `l/<expiry, big-endian>/<etcd key>` → empty. TTL index for the reaper.
- `m/compact` → the compaction revision, big-endian. See Compaction.

An etcd delete is a TiKV delete. There is no tombstone record.

### KV and Txn

Supported requests, matching what kube-apiserver sends:

- `Range`: single key, or key range with `limit`, `revision`, `keys_only`,
  `count_only`. A range read is a `Scan` at the snapshot followed by a
  `BatchGet` with `need_commit_ts` on the returned keys, to fill in
  `mod_revision`. Paging works by the caller passing the same `revision`
  with a new start key. With a `limit`, `more` is exact (scan one extra
  key) and `count` is exact (a keys-only scan of the whole range).
- `Txn` in exactly these shapes: compare one key's `mod_revision` (to 0 for
  create, to a value for update or delete) or its `version` (used by
  kube-apiserver's compactor), then one `Put` or `DeleteRange` on that key,
  else an optional `Range` on that key. The nested `Put` response carries
  its own header with the new revision.
- Bare `Put` and single-key or range `DeleteRange` (a range
  `DeleteRange` with `prev_kv` returns `UNIMPLEMENTED`).
- `Compact`.

A write runs as an optimistic two-phase TiKV transaction: read the key with
its commit timestamp, evaluate the compare, write, commit. TiKV's own write-conflict check makes the compare-and-write
atomic. On write conflict the whole operation is retried from the read, so
the compare is re-evaluated against the new state. A put with a lease also
writes the TTL index key, and removes the old index key if the expiry
changed. The data key is staged first, so it is the transaction's primary
and is committed before `commit` returns; the TTL index key is a secondary
that the client commits afterwards. A read or prewrite that meets another
transaction's lock it cannot resolve (`TxnNotFound`, "Failed to resolve
lock") is also retried from the read: it happens before this attempt's
primary commit, and the client reports any commit whose outcome is unknown
as undetermined instead.

A write is acknowledged only after this instance has delivered it. Once a
`Put`, a `DeleteRange` that deleted something, or a `Txn` whose success
branch put or deleted a key has committed at revision R, the response
waits until this instance's watch hub has applied a watermark of at least
R, that is, until R's events are queued to every watcher on this
instance. A feed reset releases the wait, because it cancels every
watcher. The wait is bounded by a 1 s deadline; on expiry the write is
acknowledged as the success it is and a warning names the revision and
the watermark. Failed compares, deletes of a missing key, `Compact`,
`LeaseGrant` and the reaper's deletes do not wait.

The guarantee is per instance. On the instance that served the write,
its events are queued to every watcher before the response is sent,
unless the deadline expired. A watcher on another instance has no
guarantee; it sees the write after about the resolved-timestamp interval,
as before. The cost is the watermark lag, paid by the writer: measured on
one store at 10ms, write latency went from 11.6 / 22.7 ms (p50 / p99) to
11.8 / 24.4 ms sequentially, and from 20.0 / 35.3 ms to 34.5 / 36.4 ms with
16 concurrent writers.

### Watch

Each `tikvd` instance runs one CDC consumer covering the `k/` range:

1. Discover the regions covering the range from PD and open one `EventFeed`
   stream per store, registering each region from a `checkpoint_ts` taken
   at startup.
2. Turn rows into committed events. A `COMMITTED` row is complete on its
   own; incremental-scan rows arrive this way. Live writes are two-phase,
   so the main path is the pair: a `PREWRITE` row (which carries the value and
   the old value) is held until its `COMMIT` row (which carries only the
   commit timestamp) with the same key and start timestamp arrives, and
   dropped on `ROLLBACK`. `INITIALIZED` marks the end of a region's
   incremental scan. Request old values so `prev_kv` and delete events can
   be populated; for a two-phase write the old value is the one from the
   `PREWRITE` row. A put event is a create when its record's `version` is
   1, the same rule the KV reads use.
3. Hold committed events in a sorter ordered by commit timestamp,
   deduplicated by key and commit timestamp, because re-registering a
   region replays events above its last resolved timestamp.
4. Track the resolved timestamp per region. The watermark is the minimum
   across all regions covering the range. Release events at or below the
   watermark, in commit-timestamp order, to a bounded in-memory window and
   to subscribed watchers, as soon as the CDC message (or region lookup)
   that moves the watermark has been applied.
5. On a region error (leader moved, split, merge, epoch change), look the
   range up again in PD and re-register the affected regions from that
   range's last resolved timestamp. The watermark does not advance past a
   range that has no live registration.
6. When a region's resolved timestamp falls behind by more than a
   threshold, scan that region's part of the `k/` range for locks older
   than the threshold and resolve them with the client's lock-cleanup
   calls. Every write commits in two phases, so a `tikvd` that crashes
   mid-commit leaves a lock that would otherwise hold the watermark back
   for every instance. Scoping the scan to `k/` is enough because of
   fact 8. The lag check runs once a second.

A stalled watermark is safe: watches and progress notifications pause, and
nothing wrong is delivered. An instance whose watermark has been stalled
past a second threshold reports not serving. If the sorter exceeds its
memory bound, or a registration's checkpoint is below the compaction
revision (TiKV v8.5.6 does not report this itself), the instance cancels all its watches with etcd's compacted
error and restarts its subscription from a fresh timestamp.

A client watch with start revision R:

- R within the window: replay from the window, then follow live.
- R is 0: fetch a timestamp T from PD and deliver events above T.
- R older than the window, or older than the instance's start: fail the
  watch with etcd's compacted error. kube-apiserver responds by relisting.
- R within the window, but the replay from R would exceed the per-watcher
  queue bound: fail the watch with etcd's compacted error.

Progress notifications and `WatchProgressRequest` report the watermark.

Watch delivery lags a commit by up to TiKV's resolved-timestamp interval
plus the TSO and `check_leader` round trips TiKV makes to advance it;
`tikvd` adds no wait of its own. On the instance that served a write, the
write's response waits out that lag instead (see KV and Txn). The deployment guidance is to set
`cdc.min-ts-interval` to 10ms (fact 9). TiKV and PD spend more CPU the
lower it is.

### Leases

kube-apiserver uses leases only to give keys a TTL. `LeaseGrant` returns an
ID that encodes the TTL and stores nothing. A put with that lease computes
the expiry from PD time, as its transaction's start timestamp rounded up
to a whole second plus the TTL, and writes it in the record and the TTL
index. Every instance runs a reaper that scans the index up to the
current PD time, rounded down to a whole second, and deletes
expired keys through the normal write path, conditional on the record still
carrying that expiry. Racing reapers conflict harmlessly, and watchers see
ordinary delete events. `LeaseRevoke` and `LeaseKeepAlive` are not needed
by kube-apiserver and return `UNIMPLEMENTED`.

### Compaction

TiKV is not relied on to reject a read below its GC safepoint, so `tikvd`
keeps its own compaction revision in `m/compact`.

- `Compact(R)` rejects a non-positive R as compacted and an R above the
  current timestamp as a future revision. Otherwise it reads the previous
  compaction revision P, rejects an R at or below P as compacted, writes
  R to `m/compact`, and then advances TiKV's GC safepoint to P, not to R.
- GC therefore runs one compaction behind. A read that passed the check
  just before a compaction has a whole compaction interval (five minutes
  under kube-apiserver) before its data can be collected.
- A read at an explicit revision reads `m/compact` at a fresh timestamp and
  returns etcd's compacted error if its revision is below it. A read "now"
  needs no check.

### Maintenance and serving

`Status` reports a fixed etcd version of 3.5.13 (new enough for
kube-apiserver to enable watch-progress requests) and the current
watermark as its revision. gRPC health reports serving once the CDC
consumer has a watermark. TLS is optional, enabled by certificate flags.

### TiKV client

`tikvd` depends on a vendored copy of `tikv-client` under `vendor/`, with a
small patch that adds commit-timestamp-returning `get` and `batch_get` and
makes the generated protobuf types and the PD region lookup public. The CDC
consumer uses those types. The patch is kept small enough to offer
upstream. (Builder's call, made because nothing is published outside the
repo this way.)

## Known limits

- Watch and progress lag by up to `cdc.min-ts-interval` plus TiKV's round
  trips to advance it: 5.6 ms p50 and 11.3 ms p99 at 10ms on one store
  (fact 9). A writer pays that lag in its write latency, and in return its
  write is already queued to every watcher on the same instance when the
  response arrives. A client that writes through one instance and watches
  through another can still act on its watch cache before its write
  arrives there.
- A stalled feed delays every changing write on that instance by up to the
  1 s delivery deadline, and such a write is acknowledged without the
  same-instance guarantee.
- Each instance holds its own CDC subscription to every region in the
  range, so CDC load on TiKV grows with the number of instances.
- `count` on a paged list rescans the range's keys on every page.
  kube-apiserver only uses it as an estimate, so an approximate count is
  the upgrade.
- Restarting an instance makes every client watching through it relist.
- A watch resuming from before an instance's in-memory window is told to
  relist. Replaying older history from TiKV via a CDC incremental scan is
  the upgrade if relists become a problem.
- Range reads cost a scan plus a batch get.
- `count_only` scans the whole range.
- Requires a TiKV version whose `Get`/`BatchGet` honour `need_commit_ts`.
- TTL is counted from each put by PD time, in whole seconds, rounded up,
  so a key lives at least its TTL; the rounding and the reaper's
  one-second interval can add about two seconds.
- `KeyValue.lease` is the key's expiry time in unix seconds, not a lease
  id. A lease id is its TTL in seconds, and a put accepts any positive id
  as a TTL. A TTL whose expiry could not be represented in an `i64` is
  rejected.
- `prev_kv.mod_revision` in watch events is always 0.
- A `Range` at an explicit revision reports that revision in its header,
  not the current one.
- Connections from `tikvd` to PD and TiKV are plaintext.
- A single read that is still running when two later compactions commit
  can read collected data, because GC runs one compaction behind. A CDC
  incremental scan running in the same window can too.
- `Compact` returns an error when only its gc step failed, although the
  compaction took effect.
- A range delete is one in-memory transaction and can miss a key created
  in the range while it runs.
- The watch window is bounded by 100,000 events and by 256 MiB; each
  watcher's queue by 1024 batches and by 64 MiB. A watcher that passes
  either queue bound is cancelled.
- A watch whose replay from the window would exceed the per-watcher
  bound is told to relist (cancelled as compacted).

## Conformance

Run on 2026-11-19 by `CONFORMANCE=focus|full dev/acceptance.sh`: k3s
v1.36.2+k3s1 (one server, two agents) against two `tikvd` instances on
one PD and one TiKV v8.5.6, Hydrophone v0.8.0, image
`registry.k8s.io/conformance:v1.36.2`, 16 Ginkgo processes, `[Serial]`
specs run alone at the end. The baseline (`CONFORMANCE_BASELINE=1`) is
the same cluster shape on k3s's default datastore, Kine on SQLite.

- Focus, `[sig-api-machinery].*[Conformance]`, 12 min: 96 passed, 3
  failed, 7480 skipped.
- Full, `[Conformance]`, 32 min: 439 passed, 7 failed, 7133 skipped.
  Baseline full, 29 min: 446 passed, 0 failed, 7133 skipped.
- Gates after each run: no `UNIMPLEMENTED`, no feed reset, cache
  consistency asserted (190 checks focus, 486 full, none failed).

Every failed test passed on the baseline, both alone and in its full
run. Run counts below are over the focus run, the full run and the
re-runs of failed tests on `tikvd`.

- AdmissionWebhook should honor timeout: caused by `tikvd`, watch lag;
  failed 4 of 4. The test deletes and recreates a webhook configuration
  of the same name 12 ms apart, and kube-apiserver still applied the
  deleted one.
- CustomResourcePublishOpenAPI, works for multiple CRDs of different
  groups (2 of 3), of same group and version but different kinds (3 of
  4), of same group but different versions (1 of 3);
  CustomResourceDefinition creating/deleting (1 of 3): caused by `tikvd`,
  watch lag. The CRD finalizer's retry read its stale lister, got a
  conflict and dropped the key; the CRD was removed at the finalizer's
  five-minute resync, after the test's 30 s wait.
- NoExecuteTaintManager removing taint cancels eviction: caused by
  `tikvd`, watch lag; failed 2 of 2. The test reads the node at
  `resourceVersion=0` from the watch cache 3 ms after confirming the
  taint, finds no taint and skips the removal.
- OrderedNamespaceDeletion namespace deletion should delete pod first:
  caused by `tikvd`; failed 1 of 3, only under full-suite load. The
  namespace controller first acted 2 min 18 s after the delete and
  deleted the namespace 7 min after its last pod went. Mean namespace
  queue wait was 330 s against 209 s on the baseline; mean write latency
  seen by kube-apiserver was 14 to 22 ms against 1.5 to 2.5 ms.
- ControllerRevision should manage the lifecycle of a ControllerRevision:
  caused by `tikvd`; failed 1 of 2. The DaemonSet controller deleted the
  revision the test then tried to delete, 19 ms earlier.

Re-run on 2026-11-19 after the feed began releasing from the CDC message
that moves the watermark and `cdc.min-ts-interval` became 10ms (fact 9),
same cluster shape and suite:

- Full, `[Conformance]`, 28 min: 442 passed, 4 failed, 7133 skipped.
  Gates: no `UNIMPLEMENTED`, no feed reset, cache consistency asserted
  (418 checks, none failed).
- AdmissionWebhook timeout, NoExecuteTaintManager, OrderedNamespaceDeletion
  and ControllerRevision passed.
- kube-controller-manager logged 1,140 `is not as new as written version`
  errors against 2,158 before. The write each one had not yet seen was
  44 ms old at p50 and 111 ms at p99 when logged (98 and 262 ms before),
  measured from the write's commit timestamp, which trails wall clock.
- Mean `etcd_request_duration_seconds` at the end of the run: create
  27.8 ms, update 24.8, delete 16.7, get 1.5, list 3.7.

All four failures are one race in kube-apiserver's CRD finalizer, each
failing a CRD delete after 30 s. Re-run alone twice each: CRD
creating/deleting failed 1 of 2, CRD listing 2 of 2, CustomResourcePublishOpenAPI
same group and version but different kinds 2 of 2, same group but
different versions 1 of 2.

- The finalizer's first list of the custom resources creates their watch
  cache and fails with "storage is (re)initializing"; this happens on the
  Kine baseline too. It writes a failure condition and retries 11 to
  238 ms later from its CRD lister. If the lister has not yet seen that write,
  the retry's status update conflicts and the finalizer drops the key;
  the write itself changed only the Terminating condition, which the
  finalizer does not requeue on. The CRD is then deleted at the five-minute
  resync.
- Over the full run and the re-runs the finalizer hit this 14 times.
  The 4 retries that wrote recovered; the 10 that did not are exactly
  the 10 failed test runs. The losing retries came 13 to 238 ms after
  the write, the winning ones 11 to 31 ms. Closing this race needs the lister to see a write within a few
  milliseconds of its response, which a resolved-timestamp watermark at
  10ms does not give.

## Order of work

0. **Spike, throwaway.** Against a local TiKV + PD in Docker, confirm the
   four things this design has not yet seen running: CDC with
   `kv_api = TiDB` delivers events for keys written by `tikv-client` under
   an arbitrary prefix; how keys and range bounds are encoded in CDC
   requests and events; the resolved timestamp advances at a lowered
   `min-ts-interval` on an idle cluster; and which TiKV release first
   honours `need_commit_ts`. If any of these fails, stop and come back to
   the design.
1. KV and Txn over the patched client.
2. CDC consumer and Watch.
3. Leases, compaction, status.
4. k3s and Argo Workflows acceptance, with two instances.

## Left to the builder

Crate layout, error types, flag names, the size of the watch window, retry
and backoff policy, record encoding, logging and metrics, and how the local
TiKV test cluster is started.
