<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/brand/logo-wordmark-reversed.svg">
    <img src="docs/brand/logo-wordmark.svg" alt="kahshe" width="300">
  </picture>
</p>

<p align="center">
  <b>A selective scan should open the files that can match — and nothing else.</b>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/license-Apache--2.0-12727C" alt="License: Apache-2.0">
  <img src="https://img.shields.io/badge/status-pre--release-647579" alt="Status: pre-release">
  <a href="#measured-results"><img src="https://img.shields.io/badge/httplogs%20needle-2%20of%20991%20files-8A5A0F" alt="httplogs needle: 2 of 991 files read"></a>
</p>

**kahshe** (pronounced *KAW-shee*, after Kahshe Lake) is an index-aware planning proxy for Apache
Iceberg REST catalogs. Put it in front of your catalog (Polaris, Nessie, any Iceberg REST
implementation) and selective scans open fewer files — no new table format, no data migration, and
indexes that live beside your data as rebuildable Parquet sidecars. Iceberg 1.11+ clients need no
configuration at all; Trino today needs two overlaid classes, which
[the table below](#will-it-work-with-your-stack) is explicit about.

```
engines ──REST──▶ kahshe ──▶ your existing catalog
                    │
                    └─ serves /plan (server-side scan planning) with
                       stats + bloom + term-index pruning, plus /_count
```

<p align="center">
  <img src="docs/evidence/httplogs-kahshe.svg" alt="With kahshe the term index selects 2 of 991 files" width="760">
</p>

<p align="center"><em>One equality predicate over 247M rows. Stock Spark opens all 991 files in 6.6–11.5 s; the same Spark with its catalog URI pointed at kahshe opens 2 and answers in 0.34–0.44 s warm. <a href="#measured-results">The other three arms, what the index cost, and the caveats &rarr;</a></em></p>

## Why it works

Your engine sends a filter, the catalog answers with a file list. min/max statistics prune that list
when the predicate has bounds — and cannot touch `LIKE '%needle%'`, a regexp, or a value whose range
every file spans. Those queries read the whole table.

Iceberg's REST spec already has the hook: server-side scan planning (`planTableScan`). kahshe implements
those endpoints, advertises them in `/v1/config`, and injects `scan-planning-mode=server` into
`LoadTableResponse`, so a stock Iceberg 1.11+ client flips to server planning on its own. Planning
consults three Parquet sidecar tiers — character-gram blooms, an exact gram layer, a term dictionary —
and returns only the files that can match; everything else is forwarded to your catalog unchanged. The
lineage is ClickHouse-style data skipping (`ngrambf_v1`) brought to open Iceberg tables, drawn on a
ten-file table in [docs/diagrams/](docs/diagrams/DIAGRAMS-README.md). So what's the trick? There isn't
one, and that is checkable: the acceleration is an Iceberg REST endpoint the spec already defines,
answered from an index the engine never hears about. **Delete every index file and queries return the
same rows, more slowly.**

## The invariant

> **A data file is SKIPPED only when it certainly cannot match. On any error, doubt, partial state
> or staleness, the file is KEPT.**

Most index systems must be *complete* to be *correct*: a document missing from the index is missing from
results. kahshe runs the other way. A file the index does not know about is kept, so an index that is
stale, partial, unreadable or absent costs scans and never rows — engines re-apply their own filters to
what they read, as in any Iceberg scan. That asymmetry is what makes the rest operable: a window of a
table indexed (`kahshe.index.scope`), a tier switched off, a bloom made smaller are cost dials with no
correctness cliff. It also sets a trap, since every failure here looks like "the data was less selective
than you thought" — hence staleness is a gauge rather than a silent condition, and a malformed index
scope fails the build loudly rather than defaulting, invisibly, to all.

| If this happens | What a query sees |
|---|---|
| the index is stale, partial, unreadable, never built, a tier off, or coverage narrowed to a window | every file that cannot be ruled out is kept — the same rows, more files scanned, never a wrong row |
| the response rewrite fails | passthrough unmodified: the client sees its catalog and plans locally (`kahshe_response_rewrite_failures_total`) |
| the snapshot carries delete files | server planning is neither advertised nor served; the client plans locally — correct, unaccelerated |
| the backing catalog is unreachable | `/readyz` goes false within seconds, on its own port and executor, so probes answer while the data plane is saturated |
| the kahshe process is down | catalog calls fail. This is the real blast radius — kahshe is in the metadata path, so front it the way you front the catalog itself. There is nothing to drain first: no plan store, no paging, no cross-request continuation, no state shared between replicas |

**Getting it out again.** Point the catalog URI back at your catalog. That is the whole removal: no
metadata to unwind, no format to convert, no data to migrate, no engine change to revert because there
never was one. What is left is an `_index` directory of Parquet files your table metadata never
referenced and an inert `kahshe.index` property. The exit is one config line. Adoption is that line plus the handful of decisions in
[Deploying it](#deploying-it) — TLS, the cache TTL above one replica, and where the index root lives.

## Quickstart

Docker and a clone. There is no published image yet, so the first run compiles the modules inside the
container — **budget 5-10 minutes cold**, seconds afterwards. It brings up Apache Polaris as the backing
catalog with kahshe in front of it, then seeds a four-file demo table whose `msg` column is indexed:

```bash
git clone https://github.com/Kahshe-io/kahshe.git && cd kahshe
docker compose up -d --build
docker compose run --rm seed
```

Ports 8282 and 8283 must be free. A kahshe started locally from `build/install` holds both, and docker
reports that as `ports are not available` rather than naming it.

The seeder waits for the automatic indexer and prints verified plan results:

```
verified plan results (files returned out of 4):
  no filter                              -> 4
  id >= 3000 (min/max stats)             -> 1
  contains 'timeout' (3-gram bloom)      -> 1
  contains 'zzz-absent' (3-gram bloom)   -> 0
```

The substring cases are the point — min/max stats can never prune those. `docker compose down -v` resets it.

**Checking the rest yourself.** `io.kahshe.ClientDemo` proves the stock-client flip against that stack —
an unmodified Iceberg 1.11+ Java client loading `RESTTable` and planning server-side:

```bash
./gradlew :app:installDist && java -cp 'app/build/install/kahshe/lib/*' io.kahshe.ClientDemo http://localhost:8282
``` The exact SQL
behind every number below is in [benchmark/evidence/](benchmark/evidence/), with per-query records as
JSONL beside it. Those records are all Trino runs, which covers every ClickBench timing below. The
four httplogs arms below are Spark, driven by `spark-needle.py`, also there; it prints its per-query
results to stdout and nothing captures them, so those wall-clock ranges and the three copy sizes are
transcribed from the run log rather than read back from a committed record. The launcher that drove
either job on the authors' cluster is not here.
`format/src/test/resources/conformance/` is the fixture a second implementation of the format would
validate against — none exists yet.

## Will it work with your stack

| | what you do | what happens |
|---|---|---|
| **Your catalog** — Polaris, Nessie, any Iceberg REST implementation | point the engine's catalog URI at kahshe | everything kahshe does not serve itself is forwarded unchanged |
| **Spark 3.4–4.1, Flink batch** — the Iceberg 1.11+ Java client, unwrapped | nothing | the client reads `scan-planning-mode=server` from the table and plans server-side on its own. Measured with Spark 3.5.3 + Iceberg 1.11.0. Batch reads only: structured streaming and start/end-snapshot reads plan locally |
| **PyIceberg 0.12, Daft** | one catalog property: `scan-planning-mode: server` (`rest-scan-planning-enabled: true` on 0.11) | PyIceberg never reads the per-table setting, so kahshe's injection is invisible to it until the next release makes it zero-config. **The property is catalog-wide**: a client that honours it will ask kahshe to plan merge-on-read tables too and get a 422, so on a catalog holding any, either set `KAHSHE_SERVE_DELETE_BEARING=true` (see [deploying it](#deploying-it)) or wait for that release. Equality, `IN`, prefix and ranges prune; Daft drops a filter containing anything else. **Polars' native reader crashes on a filtered scan under server planning** — it dereferences per-file statistics kahshe strips; do not point it at kahshe yet |
| **Snowflake, StarRocks, Doris, ClickHouse, Presto, Dremio, DuckDB releases** | nothing | passthrough — they take the URL and plan from manifests themselves. Everything works, nothing gets faster. Presto flips the day it bumps to Iceberg 1.11; DuckDB's main branch already honours the injection |
| **BigQuery, Athena, Databricks, Fabric** | — | cannot sit in front: none accepts a user-supplied REST catalog URI for reads. Athena is on Iceberg 1.4.2 |
| **Trino 483** | drop in two overlaid classes ([`dev/trino-patch/`](dev/trino-patch/README.md)) | Trino bundles Iceberg 1.11 but rewraps the loaded table and discards its REST scan planning; `TrinoRestCatalog` restores it, and `IcebergSplitSource` recognises the token-boundary `regexp_like` Trino cannot otherwise push into an Iceberg expression. The scan-planning fix is open upstream as [trinodb/trino#30891](https://github.com/trinodb/trino/pull/30891); **the kahshe arm of the ClickBench comparison below carried both overlaid classes**; its baseline arm is an unmodified Trino 483 pointed straight at the catalog, so the 1,000-file, 8,180 ms side is what stock Trino gives you |
| **Merge-on-read tables** | `KAHSHE_SERVE_DELETE_BEARING=true`, unless Trino reads through the proxy | off by default, a snapshot carrying delete files plans locally. With the flag on, Spark and Flink plan server-side and return correct rows — measured, position and equality deletes both. Leave it off while Trino 483 reads through this proxy: it throws on a sequence number the REST scan-task format drops. Per snapshot, not per table — a compacted, delete-free snapshot serves either way |
| **Your tables** | `ALTER TABLE logs.events SET PROPERTIES ('kahshe.index' = 'msg')` | no rewrite, no new table format, no index DDL. Parquet sidecars beside the data, rebuildable at any time |

Strings, integers, decimals, UUIDs and binary all index — a non-string column by its canonical string
form, so a plain `=` or `IN` from any stock engine prunes through the same tiers. One limit up front: a
snapshot carrying **delete files** is not served by default, so a merge-on-read table falls back to
local planning until compaction.

## Measured results

Two independent runs, kept apart — not averaged, not combined, and every figure the animated scenes carry
is also in the text here. Raw records, SQL and runners: [benchmark/evidence/](benchmark/evidence/); how
each scene is drawn, and what it does not: [docs/evidence/](docs/evidence/EVIDENCE-README.md).

### Equality on a high-cardinality column — httplogs

`SELECT count(*) FROM logs.httplogs WHERE clientip = '71.162.18.0'` — 2 rows in 247,249,096, across 991
files and 1.31 GB. Spark 3.5.3 + Iceberg 1.11.0, `local[8]`, 10 GB driver, one job at a time, n=3.

| arm | what it is | files read | wall clock |
|---|---|---|---|
| stock | the table as loaded | 991 of 991 | 6.6–11.5 s |
| plain | Spark-written copy, no blooms | 991 of 991 | 18.4–20.5 s |
| bloom | same copy, `write.parquet.bloom-filter-enabled.column.clientip=true`, fpp 0.01 | 991 of 991 | 11.3–12.4 s |
| **kahshe** | same Spark, catalog URI swapped to kahshe | **2 of 991** | **0.34–0.44 s** warm, 2.8–3.8 s cold |

**How to read the bloom arm.** Blooms are judged against `plain`, not `stock`: both Spark-written copies
are bigger than the table they came from — 3.00 GB and 4.05 GB against 1.31 GB — so neither is
comparable to the untouched table. Inside its own copy the bloom did real work, 18.4 s → 11.3 s, and
that it still opens all 991 files is no defect in the measurement: a Parquet bloom prunes row groups
*inside* a file, which cannot happen until the file is open. Those pages cost 1.05 GB, 35% added to that
copy; kahshe's tiers on the original cost 12.1 MB — term 11,509,403 B (0.88% of the data, 1,149,519
terms), bloom 566,420 B (0.04%), [drawn here](docs/evidence/httplogs-overhead.svg) — whose term tier merges in ~70 s on 12 threads after a
full read of the data, and the plan call is 17–18 ms warm, 320–540 ms cold. Warm: **15–34× faster than stock, 26–36×
faster than the bloom arm, for about 1% of the storage.**

### A predicate min/max pruning cannot touch — ClickBench

`regexp_like(lower(URL), '(^|[^a-z0-9])offilialog([^a-z0-9]|$)')` — 6 rows in 100M ClickBench hits as
shipped, unsorted on `URL`; Trino two nodes, arms alternated per round, a fresh
pod per arm, the class overlay on the kahshe arm only. Nothing prunes a regexp, so all 1,000 files survive Iceberg's own pruning: the case with
nothing to compare against rather than something slower. The term index takes it to **4 files**, scanning
**6.8 MB rather than 2,440.1 MB** and 400,000 rows rather than 99,997,497, for the same six rows —
planning 24 → 30 ms, execution 8,180 → 3,000 ms on node 003
([clickbench-kahshe.svg](docs/evidence/clickbench-kahshe.svg)). About 2 s of each execution is Trino's
per-query floor rather than scan time, so what moved is the scan portion: roughly 6,180 → 1,000 ms. Node
003 is the conservative pair on every axis, not just the ratio: it supplies the lower stock baseline
and the slower kahshe result. 001 ran 9,920 ms stock and 2,170 ms through kahshe — a 4.6x gap on
execution against 003's 2.7x. 001 was slower stock
(9,920 ms) and faster through kahshe (2,170 ms), a 4.6× gap on execution against 003's 2.7×. Index cost
on `URL`: term 0.84%, bloom 0.33%.

### What these numbers do not say

- **The httplogs byte and row figures are inferred**, from files × size; Spark's per-run input bytes were
  not captured, so they are approximate and must not be quoted as measured. No planning/execution split
  there either — Spark local mode gives none, Trino does.
- **The gram tier's size was not recorded** for either column, and lookup bytes were not measured. The
  term and bloom figures are the whole of what was measured.
- **Confidence is low-n.** httplogs is single-node, n=3; ClickBench n=1 per node with three warm re-runs
  on 003. Lab runs on the hardware described, not an installation serving traffic.

## When it will not help

Iceberg's own partition and min/max pruning runs first, so a query narrowed to one day of a partitioned
table gains nothing, and a full scan has nothing to prune. Pruning is also the whole product: kahshe
returns file references, never rows, and reads no data file on any serving path — nothing to rank or size.

Identifier-dense text defeats the gram tiers outright: there are only 4,096 possible hexadecimal
trigrams, so essentially every file of hex trace ids holds all of them. On a lab corpus of 48 GB and 2.1
billion rows of that shape, `contains` kept **220 of 220 files** for the most selective query it can pose
— a run predating the evidence records published here, so read it as the reason the term dictionary is
not optional for such a workload rather than as a headline figure. That is a limit of the approach, not
of this implementation; `kahshe_gram_saturated_files_total` counts those files.

## Deploying it

Point engines at `http://kahshe:8282` instead of the catalog — `https://` once TLS is configured, the
first row below — and that is the whole integration. Four settings below are decisions rather than
tuning; the rest have working defaults.

```bash
KAHSHE_BACKEND=http://your-catalog:8181/api/catalog KAHSHE_CREDENTIAL=client:secret \
  app/build/install/kahshe/bin/kahshe      # ./gradlew installDist first; :8282 data, :8283 admin
helm install kahshe ./helm/kahshe --set backend.url=http://nessie:19120/iceberg
```

| Setting | Why it is not optional |
|---|---|
| **TLS** | set `KAHSHE_TLS_CERT` and `KAHSHE_TLS_KEY` — the PEM pair a `kubernetes.io/tls` Secret already holds — and kahshe serves TLS itself, with optional mutual TLS and rotation without a restart. Left unset, port 8282 is plaintext HTTP carrying bearer tokens |
| `KAHSHE_TABLE_CACHE_TTL_MS=0` | required above one replica. The record of which snapshot this proxy last forwarded is per-process, so a replica that did not serve a table's `loadTable` could otherwise plan from a view of unknown age |
| `KAHSHE_INDEX_ROOT` | move the index root outside the table location if maintenance runs `remove_orphan_files` — index files are not referenced by table metadata and would be collected |
| `KAHSHE_SERVE_DELETE_BEARING` | leave `false` while any Trino reads through this proxy. Trino 483 unboxes a sequence number the REST scan-task format drops and throws, while stock iceberg-java readers are correct (measured, both delete kinds) |

**Alert on `kahshe_index_max_behind_seconds`.** Index maintenance can stop without failing — no traffic
reaches a replica, the indexer is off, builds throw — and because a stale index is still *correct*, that
gauge is the only signal that says so. Two rules go with it: alert on the process that **builds**, since
one with the indexer off reads 0 by design and a gauge climbing on a healthy pod is the page that gets
muted; and alert **per replica, never on a sum**.

Indexing itself is a table property kahshe observes in passing `loadTable` traffic, keeping the column
current in the background; every knob under it resolves per column, then per table, then from the
deployment default, and all are cost dials. A build that crashes halfway costs nothing: the previous
generation keeps serving, nothing partial is published, and the lease held beside the column's index
metadata is released by a shutdown hook — or treated as abandoned after twelve hours when a process dies
without running its hooks — so a pod killed mid-build blocks nothing. Long builds set `checkpoint-files`
to publish and resume in passes.

## Watch — detection on data as it lands

The same binary can evaluate detection rules against new data files, with no ingest pipeline and nothing
copied: the detection runs where the data already is. A row scan reads exactly the columns the rules name
out of each newly added file and evaluates **per row** — so a rule spanning columns means one row
satisfying every field, not one file holding a row for each — and rules the index can answer ride the
index build instead, for free.

```yaml
- id: moriya
  severity: critical          # info|low|medium|high|critical, required
  prefix: lakehouse           # catalog prefix, as engines address it; required
  table: logs.events
  condition: all-of           # every entry, on one row
  where: [{ column: event_id,     equals: [7045] },
          { column: service_name, contains: [ZzNetSvc] }]
```

Every alert carries the Trino SQL, snapshot-pinned, that an operator runs under their own authorization
to see the rows — kahshe never executes SQL and never returns row content. Sigma libraries convert:
[`pysigma-backend-kahshe`](sigma/README.md) compiles Sigma YAML into these rules and **refuses** any
construct kahshe has no operator for, naming it — a rule that converts with a clause silently dropped
loads clean, reviews as correct, and fires on the wrong rows. A `KAHSHE_MODE=watch` instance needs no
proxy, no indexer and no index, and loads any Iceberg `Catalog` by class name, so this runs on Glue, Hive
and JDBC too. The rule language, window rules and the alert payload: **[docs/WATCH.md](docs/WATCH.md)**.

## Security model

Passthrough requests are authorized by the backing catalog exactly as before. Served endpoints (`/plan*`,
`/_count`) verify that the caller's own bearer token can load the table from that catalog — the backend
stays the sole authority. The verdict is then memoized, keyed by a hash of the token and never the token
itself, for `KAHSHE_AUTH_CACHE_TTL_MS` (default 60 s, capped at the token's own expiry when it is a
readable JWT), so revocation lags by at most that TTL; a denial is cached for 2 s so a bad-token storm
does not amplify into the backend. Planning then runs under kahshe's service credential by default, so
sub-table controls (column masking, row filters) are not honored there;
`KAHSHE_PLANNING_IDENTITY=caller` runs every metadata read under the caller's own token instead. What that
leaves disclosed — file locations to any authorized planner, `_count` as a content oracle at table-level
authorization — is in [docs/OPERATIONS.md § Security posture](docs/OPERATIONS.md#6-security-posture).
kahshe serves TLS natively (`KAHSHE_TLS_CERT`/`KAHSHE_TLS_KEY`, mutual TLS optional, certificates re-read on rotation); unconfigured, port 8282 is plaintext HTTP carrying bearer tokens.

## Status

**Pre-release. Not yet suitable for production traffic, and not running in production anywhere.** Plan
caching, the bloom, gram and range-partitioned term tiers, `_count`, declarable index scope and the watch
role all work, under 527 tests plus 6 for the Trino overlay and 25 for the pySigma backend. Missing for a
release, plainly: no shipped SQL syntax expresses token `match` intent against a tokens column
(whole-value `=`, `IN` and `LIKE 'x%'` already reach the dictionary; token intent needs the filter
extension or an engine plugin); Trino needs the class overlay until #30891 lands; and window rules in
watch are a prototype.

The scan-planning endpoints are a standalone implementation of the Iceberg REST spec's server-side
planning API; the term and gram layers are an experimental prototype of the index family the community's
draft index spec defers to future extensions; and `contains` / `match` are extensions, clearly marked,
off the standard grammar's happy path.

## Where to read next

| | |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) · [docs/FORMAT.md](docs/FORMAT.md) | how it fits together — request path, build, invariant, module map; then the on-disk format, normatively |
| [docs/CONFIGURATION.md](docs/CONFIGURATION.md) · [docs/ENDPOINTS.md](docs/ENDPOINTS.md) | every `KAHSHE_*` variable and `kahshe.*` property; the served surface, filter extensions and `_count` |
| [docs/OPERATIONS.md](docs/OPERATIONS.md) · [docs/WATCH.md](docs/WATCH.md) | metrics, staleness alerting, index lifecycle, deployment shapes, security posture; then the rule language |
| [CONTRIBUTING.md](CONTRIBUTING.md) · [helm/kahshe/](helm/kahshe/README.md) · [sigma/](sigma/README.md) | toolchain, the gate, running it locally · the chart and its shapes · the pySigma backend |
| [benchmark/BENCHMARK.md](benchmark/BENCHMARK.md) | methodology, and the superseded OpenSearch 3.8 comparison; raw records in [evidence/](benchmark/evidence/) |
| module READMEs | [common](common/README.md) · [analysis](analysis/README.md) · [format](format/README.md) · [indexer](indexer/README.md) · [proxy](proxy/README.md) · [watch](watch/README.md) · [app](app/README.md) |

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE). Apache, Apache Iceberg, and Iceberg are
trademarks of the Apache Software Foundation.
