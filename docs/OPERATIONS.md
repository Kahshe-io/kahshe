# Operating kahshe

What to alert on, what the metrics mean, how the index behaves over a table's life, and the
shapes a deployment takes.

Everything here rests on one invariant: **a data file is skipped only when it certainly cannot
match; on any error, doubt, partial state or staleness, the file is KEPT.** So none of the
failures below produce wrong rows. They produce slower queries, which is why they need a gauge —
a silent cost is the one that never gets noticed.

---

## 1. The one metric to page on

**`kahshe_index_max_behind_seconds`.**

Index maintenance can stop without failing: no `loadTable` traffic reaches this replica, the
indexer is disabled, or every build throws. None of those move a build counter, so counters alone
cannot tell "up to date" apart from "quietly stopped indexing".

This gauge is the largest gap, over the tables this process has observed declaring `kahshe.index`,
between now and the moment a snapshot that has not yet been built was first seen. It reads 0 when
every observed table is current and grows without bound while maintenance is stopped. Any
non-zero value means at least one table is serving an index that predates its data; a rising one
means nothing is closing the gap.

`kahshe_index_max_behind_seconds > 900` for a few minutes is a good page. The gauge is in
**seconds** and `KAHSHE_INDEX_STALE_WARN_MS` is in milliseconds: 900 s is three times the default
`300000` ms, so the WARN is in the log well before the page fires.

**Two rules go with it.**

- **Alert on the process that builds.** A process whose indexer is off (`KAHSHE_INDEXER=false`: a
  proxy replica in the split shape, a watch instance that only scans) reads **0** for
  `kahshe_index_max_behind_seconds` and `kahshe_index_tables_behind`, whatever it has observed.
  It cannot close the gap, so the gap is not its to report — and a gauge climbing without bound
  on a pod that is healthy by configuration is the page that gets muted.
  `kahshe_index_tables_tracked` still counts what it saw.
- **Alert per replica, never on a sum.** Every gauge here describes what that process has
  observed since it started, and a restart resets it.

Supporting gauges: `kahshe_index_tables_tracked` (tables observed since this process started),
`kahshe_index_tables_behind` (how many have an observed snapshot no build has covered), and
`kahshe_index_last_build_age_seconds` (age of the most recent completed maintenance pass, where a
verification that the artifacts already cover the snapshot counts as a pass). That last one reads
0 both when nothing has been built yet and when a build just finished, so read it alongside the
other two.

One counter belongs with them: `kahshe_indexer_jobs_dropped_total`, builds dropped because the
indexer queue was full. A drop skips that table's snapshot without failing, so no build counter
moves and no other metric records it; the drop does WARN, naming the queue depth, the table and the
snapshot, and that log line is the only thing that says *which* table the gauge is counting. The
claim is not recorded either, so a later observation of the same snapshot retries.

---

## 2. The rest of the metrics

A selection, not the whole set: `GET /metrics` on the admin port (8283) emits every series.

The log is the other signal, on stderr. `KAHSHE_LOG_FORMAT=json` (the chart's `logging.format`)
makes it one JSON object per line — `ts`, `level`, `logger`, `thread`, `msg`, `exception` — so a
collector indexes the per-file build lines and the per-plan lines by field instead of parsing the
text form.

### What each index tier pruned

`kahshe_prune_files_in_total{tier}` and `kahshe_prune_files_kept_total{tier}` — one pair per
index type, recorded on every turn of the pruning
cascade. The label is the type's own key (`aggregate` for the term tier, `grams`, `bloom`, or whatever a
deployment's own type calls itself), because the types are a `ServiceLoader` seam and a fixed field per tier
would be wrong the day someone adds one. Nothing is emitted at all until a tier has taken a turn.

The tiers chain nose to tail — what one keeps is what the next is handed — so the ratio per tier
says which one is doing the work:

- **`aggregate` kept ≈ in.** The token predicate matched nearly every file, so either the term is
  common or the column's analyzer is cutting it too coarsely. A whole-value contract
  (`kahshe.index.<column>.analyzer = value`) is the cheap lever here, and it is the one that took
  a lab query from 991 files to 2.
- **`bloom` kept ≫ what `aggregate` would have kept.** Gram density, not index failure. A larger
  gram size (`kahshe.index.<column>.ngram = 4`) is the lever.
- **A tier with kept = in on every plan.** It is switched off for every column, or it refused its
  leaf and kept everything — check the WARNs. A registered tier takes its turn and is counted even
  when it is off, so a tier absent from the scrape means only that no plan has run through the
  pruner since this process started.

These exist because the cascade was previously unobservable: `kahshe_plan_files_total` and
`kahshe_plan_files_kept_total` are recorded once per plan, so which tier pruned, and whether a
second predicate narrowed anything at all, could only be inferred.

### Per table

Every other `kahshe_index_*` and `kahshe_plan_*` series is a fleet aggregate, which cannot say
which table is behind, whose builds are eating the indexer, or whose queries prune nothing. These
can. The `table` label is `namespace.table` — never the prefix, which can carry tenancy — and a
family is absent from the scrape until it has something to say. They carry their own names rather
than riding as labelled series under the fleet ones, so `sum()` over any one family still means
something.

| metric | what it says |
|---|---|
| `kahshe_index_behind_seconds{table}` | how long each tracked table has been behind; a current table has no line. `kahshe_index_max_behind_seconds` is the worst of these |
| `kahshe_table_index_builds_total{table,column,kind}` | publishes per column, `kind` one of `full`, `incremental`, `restamp`. A checkpointed build counts once per pass, since each pass publishes |
| `kahshe_table_index_build_seconds_total{table,column}` | wall time spent building each column, to the millisecond, so `rate()` over it is that column's share of the indexer |
| `kahshe_table_plan_requests_total{table}` | plans served per table. The fleet's `kahshe_plan_requests_total` counts requests as they arrive, before the auth gate, so it runs ahead of the sum of these |
| `kahshe_table_plan_files_kept_total{table}` | files returned per table. Divided by the requests, the average plan size — and a table whose average never falls as it grows is one the index is not pruning |

### Caches

The bloom, term, plan and gram caches are byte-weighted against the `KAHSHE_CACHE_BYTES` budget.

| metric | what it says |
|---|---|
| `kahshe_{index,term,plan,gram}_cache_weight_bytes` | each cache's estimated weight |
| `kahshe_{index,term,plan,gram}_cache_evictions_total` | capacity evictions per cache |
| `kahshe_cache_budget_bytes` | the total budget in force |

Rising evictions with flat weights means the budget is the binding constraint, not the workload.

### Build progress and the term build's bounded path

| metric | what it says |
|---|---|
| `kahshe_index_data_files_read_total` | advances as each data file lands, rather than at the end of a multi-hour build |
| `kahshe_term_build_spills_total`, `kahshe_term_build_run_merges_total` | the sorted runs the term build wrote and merged |
| `kahshe_term_lookups_total`, `kahshe_term_entry_cache_hits_total` | token-set lookups against the term index — **one per call**, whether or not a leaf was read — and how many of the tokens in them the resolved-entry cache answered. Prefix and range lookups are counted separately, by `kahshe_term_prefix_lookups_total` |
| `kahshe_index_lease_skips_total` | columns a fleet member found another member already building — work taken, not work lost, and the honest way to read a fleet larger than it needs to be |

Every data file also logs an INFO line with files done against files total, rows, source bytes
read and elapsed seconds, so the log's own timestamps give the build's rate.

### Degradations worth a dashboard

| metric | what it says |
|---|---|
| `kahshe_gram_saturated_files_total` | files whose gram set saturates its alphabet's gram space — dense identifier columns where the gram and bloom tiers cannot prune and are only costing storage. The build WARNs with the column name and the per-column properties (`gram-index`, `bloom-fpp`) that stop paying for them |
| `kahshe_gram_builds_skipped_total` | the gram layer was not shipped for a build because the in-progress map crossed `KAHSHE_GRAM_BUILD_MAX_BYTES`; that column serves from blooms |
| `kahshe_gram_too_large_total` | two causes, and the common one is not the build. A **reader** refuses a gram layer before opening it when `3 × gram-coverage.bytes` exceeds the gram cache budget — a 15% share of `KAHSHE_CACHE_BYTES`, not `KAHSHE_GRAM_BUILD_MAX_BYTES` — and that column serves from blooms until the artifact changes. A **build** also increments it when a prior gram leaf is too large to reload, but there the layer still ships: gram coverage restarts at a new `from-ordinal` and only the files below it fall back to blooms. Check which by the log line: the reader's names the cache budget, the build's names `KAHSHE_GRAM_BUILD_MAX_BYTES` and the ordinal |
| `kahshe_index_too_large_total` | a bloom tier refused before loading, because its estimated heap exceeds the bloom cache budget — a 40% share of `KAHSHE_CACHE_BYTES`. That column serves without blooms: correct, since an absent bloom keeps files, but slower, and nothing else surfaces it |
| `kahshe_watch_counts_truncated_total` | files whose per-file term map hit its cap (`kahshe.watch.max.distinct.terms`), so token (`match`) rules were **skipped** for them. Alerting has a hole: those files were indexed but their rules did not run. `contains` rules read the uncapped gram set and are unaffected |
| `kahshe_term_prefix_capped_total` | a prefix query exceeded `KAHSHE_PREFIX_MAX_TERMS` and every file was kept, loudly |
| `kahshe_response_rewrite_failures_total` | a `loadTable` response could not be rewritten, so the client saw its catalog unmodified and planned locally |
| `kahshe_errors_total` | serving errors, including the one logged stats exception below |

One deliberate exception is worth knowing about: if the two pinned-snapshot scans behind a plan
disagree about a file, the stats-laden task is **returned** rather than dropped — a WARN naming
the path, plus `kahshe_errors_total` — because dropping it would be a false negative.

### Watch

`kahshe_watch_rules_uncovered`, `kahshe_watch_suppressed_total`,
`kahshe_watch_replay_files_read_total`, `kahshe_watch_replay_truncated_total`,
`kahshe_watch_window_trips_total`, `kahshe_watch_window_key_evictions_total`,
`kahshe_watch_window_late_drops_total`, `kahshe_watch_window_keys`,
`kahshe_watch_reports_seen_total`, `kahshe_watch_reports_missed_total`,
`kahshe_watch_report_alerts_delivered_total`. What each means, and which of them count **missed
detections**, is in [WATCH.md](WATCH.md).

---

## 3. The index over a table's life

**A build that dies leaves the previous generation serving.** Nothing half-written is published. A
build holds a lease file (`build.lease`) beside the column's index metadata for its duration; a
second builder of the same column — another replica, or a CLI run beside the indexer — refuses
loudly and names the holder. In a fleet (`KAHSHE_INDEX_FLEET=true`) that same collision is a
counted skip instead (`kahshe_index_lease_skips_total`) and the table is left unclaimed, so a
later poll revisits it — refusing is right for a lone builder and wrong for one of N. A JVM
shutdown hook releases every lease the process holds, so a pod killed mid-build leaves none
behind, and a lease older than **twelve hours** is treated as abandoned, which covers a process
that died without running its hooks. For builds long enough that starting over is expensive,
`checkpoint-files` publishes and resumes in passes: each completed pass publishes a whole document
marked `partial` over a prefix of the files. That is the one thing published mid-build, and it is
safe — freshness reads it as not current so the next observation resumes, `_count` refuses it, and
pruning is indifferent because an uncovered file is kept.

**Files leaving the table are cheap.** Compaction, expiry and `rewrite_data_files` all remove data
files, and a removed file no longer forces a full rebuild: its index ordinal is retained as a
tombstone so every surviving bitmap keeps meaning what it meant, and the next build is an
ordinary incremental one. Once tombstones pass a threshold a build renumbers the survivors and
drops them, translating every bitmap in the same pass. The one visible consequence is on `_count`
(see [ENDPOINTS.md](ENDPOINTS.md#3-_count)), never on pruning.

**Rebuilds are triggered by observation, not by a schedule.** Indexing is a table property
(`kahshe.index`) kahshe observes in passing `loadTable` traffic, keeping the column current in the
background: a commit through the proxy marks the index stale and the next `loadTable` triggers the
rebuild. Every knob under that property resolves per column, then per table, then from the
deployment default, and all are cost dials
([CONFIGURATION.md](CONFIGURATION.md#per-column-and-per-table-tuning)). Changing `ngram`,
`analyzer` or `max-token-length` changes an identity the index records, so the affected column
rebuilds while the existing index keeps serving under its own rules.

**`remove_orphan_files` will delete your indexes** if `KAHSHE_INDEX_ROOT` is left inside the table
location, because index files are not referenced by table metadata. This is the one maintenance
interaction that costs something you have to rebuild rather than something that self-heals.

### Checking a table

`GET /index` on the admin port lists every table this process has seen declare `kahshe.index`
since it started; `GET /index/{prefix}/{namespace}/{table}` answers for one, or 404 when it has
not been seen (a multi-level namespace is written as on the data port, levels joined by `%1F`).
Both sit behind `KAHSHE_ADMIN_TOKEN`. Per table: the columns as last declared, `current_snapshot`
(last observed) against `indexed_snapshot` (last covered by a completed pass, null until one
has), `behind_seconds` — 0 when they agree, otherwise dated from the first uncovered observation,
the rule `kahshe_index_max_behind_seconds` uses — then per column the last build's kind (`FULL`,
`INCREMENTAL`, `RESTAMP`), snapshot, files covered, added and departed, `built_at`, duration,
analyzer and warnings; the last build failure, kept across later successes so a flapping build
shows; and the columns refused as unindexable, with the reason. A replica with
`KAHSHE_INDEXER=false` answers an empty list and a `note` saying so: it cannot build, so it is not
the one to report. The listing is per process and not durable — a restart empties it until
traffic refills it, and each replica lists only the tables whose `loadTable` passed through it.

`kahshe status <prefix> <namespace.table>` prints the same document from outside any process:
the current snapshot and the declared columns from the catalog, each column's last build from its
`build-report.json` under the index root, `indexed_snapshot` as the oldest snapshot those reports
cover (null unless every declared column has a complete build under the configured analyzer and
gram rule), and `behind_seconds` as the age of the oldest retained commit the index does not
cover — the command has no observation history, so the storage reading stands in for the
observed one, and a `sources` block in the output says so field by field. `last_failure` is not
known from storage.

---

## 4. Deployment shapes

Point engines at `http://kahshe:8282` instead of the catalog — `https://` once TLS is configured —
and that is the whole integration. Four settings are decisions rather than tuning, each in the
section that owns it: TLS (§6), the table cache TTL above one replica (below), the index root under
`remove_orphan_files` (§3), and delete-bearing snapshots with Trino (§5).

One image, roles by configuration. The chart renders all of these —
[helm/kahshe/README.md](../helm/kahshe/README.md) — and
[ARCHITECTURE.md §10](ARCHITECTURE.md#10-deployment-shapes) covers why the roles split where they
do.

| shape | what runs | when |
|---|---|---|
| **Single process** (`KAHSHE_MODE=both`, default) | data plane, admin, indexer, and the watcher when rules are configured | small installs; this is the [quickstart](../README.md#quickstart) |
| **Split** | N proxy replicas with `KAHSHE_INDEXER=false` and `KAHSHE_TABLE_CACHE_TTL_MS=0`, plus one `KAHSHE_MODE=watch` instance that discovers, builds and alerts | serving latency should never sit behind a build |
| **Detection split from building** | a `KAHSHE_MODE=watch` instance with `KAHSHE_INDEXER=false` that row-scans and delivers, while builds run anywhere else | builds are a Job, a CLI run, or another implementation |
| **Indexer fleet** | N replicas with `KAHSHE_INDEX_FLEET=true`, each taking a column's build lease, plus one watch instance that scans and delivers | one process cannot keep up |

Above one replica, in any shape, `KAHSHE_TABLE_CACHE_TTL_MS=0` is required: the record of which
snapshot this proxy last forwarded is per-process, so a replica that did not serve a table's
`loadTable` could otherwise plan from a view of unknown age.

Two consequences of any shape where the proxy does not build:

- Index freshness depends on the building deployment, and both kinds of non-building process read
  0 on the behind gauges by design. Alert on the builders.
- A watch instance maintains **only the tables its rules name** — the catalog wrapper has no
  namespace listing — so in that shape every `kahshe.index` table needs a watch rule naming it, or
  it is never built.

Fleet members neither deliver alerts (`KAHSHE_WATCH_SINK=none`; they write build reports and one
watch instance delivers from them, so dedup stays in a single process) nor run the row scan (it is
not lease-coordinated, so N members would each read every new data file). The unit of scale is the
column: N members build N columns at once, and one column's build is still one member's, so the
biggest column's build time remains the floor. `kahshe_index_max_behind_seconds` on the fleet is
the signal that it is too small.

---

## 5. Blast radius

| if this happens | what a query sees |
|---|---|
| the index is stale, partial, unreadable, or was never built | every file that cannot be ruled out is kept — slower, never wrong |
| a tier is off, or coverage narrowed to a window | the same rows, more files scanned |
| the response rewrite fails | passthrough unmodified; the client plans locally |
| the snapshot carries delete files | server planning is neither advertised nor served; the client plans locally. `KAHSHE_SERVE_DELETE_BEARING=true` lifts that refusal — leave it `false` while any Trino reads through this proxy: Trino 483 unboxes a sequence number the REST scan-task format drops and throws, while stock iceberg-java readers are correct (measured, both delete kinds) |
| the backing catalog is unreachable | `/readyz` goes false within seconds, on its own port and executor |
| the kahshe process is down | catalog calls fail. kahshe is in the metadata path, so front it the way you front the catalog itself. There is nothing to drain first: no plan store, no paging, no cross-request continuation, no state shared between replicas |

---

## 6. Security posture

- **Passthrough is unchanged**: the backing catalog authorizes exactly as it does today.
- **Served endpoints re-check the caller**: for `/plan*` and `/_count` the caller's own bearer token
  must load the table from the backing catalog, verified per request; the backend stays the sole
  authority for that check. The verdict is cached by token hash — never the raw token — for
  `KAHSHE_AUTH_CACHE_TTL_MS` (default 60 s, capped at the token's own expiry when it is a readable
  JWT), so revocation lags by at most that TTL; a denial is cached for 2 s so a bad-token storm
  does not amplify into the backend.
- **Every served plan is logged**, one INFO line in a fixed key order — `plan table=ns.table
  snapshot=N caller=<hash> files_in=N files_kept=N ms=N` — where `caller` is the first eight hex
  digits of the SHA-256 of the bearer (`none` without one): enough to tie one caller's plans
  together, never the token, and never a file path.
- **Granularity is a deployment choice.** By default the gate proves the caller can load the table and
  planning then runs under kahshe's service credential, so sub-table controls (column masking, row
  filters) are not honored on served endpoints. `KAHSHE_PLANNING_IDENTITY=caller` plans and counts
  under the caller's own token instead — caller-scoped catalog clients in a bounded LRU keyed by token
  hash — so backend authorization and vended-credential scoping apply to every metadata read.
- **Residual in `service` mode**: the per-(table, snapshot) plan cache is shared across callers who
  each passed their own load check, and file *locations* are visible to any authorized planner. Deploy
  it only where table-level read implying index visibility is acceptable.
- **Know what is disclosed.** Plan responses are stats-stripped by default (`KAHSHE_PLAN_STATS`), and
  `_count` is a content oracle — an exact token occurrence count in the indexed column for anyone
  whose token can load the table, regardless of masking or row filters at the backend. Watch webhook
  payloads carry the same class of metadata.
- **TLS.** kahshe serves TLS natively when `KAHSHE_TLS_CERT` and `KAHSHE_TLS_KEY` are set — the PEM pair a `kubernetes.io/tls` Secret holds — with optional mutual TLS and certificate rotation without a restart. See [CONFIGURATION.md](CONFIGURATION.md#tls). **Unconfigured, port 8282 is plaintext HTTP carrying bearer tokens**, which is fine on loopback or inside a trust boundary you control and is not fine anywhere else.
- **The admin port.** `/metrics` is operational telemetry — volumes, cache weights, build and alert
  counts, and per table (section 2) build, plan and freshness figures under the table's own name,
  never a prefix, a path or a token — but it is a surface anyone who can reach port 8283 can read,
  and it answered everyone until now. `KAHSHE_ADMIN_TOKEN` puts a bearer on it and on every other
  admin path; a missing or wrong token is a 401 with a challenge, counted on
  `kahshe_admin_auth_rejected_total`, and the comparison is constant-time against a digest. `/healthz`
  and `/readyz` stay open whatever is set, because a kubelet cannot easily carry a bearer and a
  health endpoint that needs a secret fails closed for the wrong reason. `KAHSHE_ADMIN_BIND` narrows
  where the port listens: `127.0.0.1` or the pod IP in production, every interface by default.
  Unset, both leave the port as it was, and startup says so at WARN.
  The image runs as uid 10001 and the chart ships a NetworkPolicy.

---

## Further reading

- [CONFIGURATION.md](CONFIGURATION.md) — every variable and table property
- [ENDPOINTS.md](ENDPOINTS.md) — the served surface
- [WATCH.md](WATCH.md) — the rule language and its own metrics
- [ARCHITECTURE.md](ARCHITECTURE.md) — the request path and the module map
