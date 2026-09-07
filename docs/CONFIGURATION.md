# kahshe configuration

Three surfaces: environment variables set on the process, `kahshe.*` properties set on an
Iceberg table, and two JVM system properties that are build-memory ceilings. Table properties win
over environment variables where both apply, within the ceiling rule below.

Most of these have working defaults. The four settings that are decisions rather than tuning —
TLS, `KAHSHE_TABLE_CACHE_TTL_MS` above one replica, `KAHSHE_INDEX_ROOT` under
`remove_orphan_files`, and `KAHSHE_SERVE_DELETE_BEARING` with Trino — are called out in the
[README's deployment section](../README.md#deploying).

---

## 1. Table properties

Indexing is declared on the table, not through DDL or an index service:

```sql
ALTER TABLE logs.events SET PROPERTIES ('kahshe.index' = 'msg,trace_id');
```

kahshe observes the property in passing `loadTable` traffic and keeps the named columns current
in the background. The CLI (`kahshe index <prefix> <ns.table> <column>`) remains for backfills,
and `KAHSHE_INDEXER=false` turns the automatic worker off.

### What can be indexed

`string`, `int`, `bigint`, `decimal`, `uuid`, `binary` and `fixed`. A non-string column is
indexed by its canonical string form — decimal text for integers, plain text with trailing zeros
stripped for a decimal, 32 undashed hex digits for a UUID, lowercase hex for binary — and a plain
`=` / `IN` predicate on it from any stock engine prunes through the same tiers with no engine
change, because the literal is canonicalised the same way. `WHERE order_id = 88213347` on an
indexed `bigint` prunes.

A string inside a struct is declared by its dotted path (`'kahshe.index' = 'attrs.msg'`) and
indexed like a top-level column; the per-column keys below use the same path. A `list<T>` or `map<K,V>` is declared
by naming the CONTAINER (`'kahshe.index' = 'tags'`) and is indexed as the union of its members'
terms — a list's elements, a map's VALUES; keys are not indexed — see FORMAT.md §6.7 for what that
lets a query ask. A leaf INSIDE one (`tags.element`, `props.key`, `props.value`) is refused at
build time, because a bitmap over file ordinals cannot answer a predicate keyed there without
losing rows. The refusal is remembered until the schema changes and counted as
`kahshe_index_columns_refused_total`; it no longer stops the table's other columns from building.

Renaming an indexed column is safe: the index is keyed by the column's Iceberg field id, and a
`kahshe.index` entry written under the old name keeps applying — the build logs a WARN asking you
to update it.

### Per-column and per-table tuning

Every key resolves per column (`kahshe.index.<column>.<key>`), then per table
(`kahshe.index.<key>`), then from the deployment default. All of them are cost dials with no
correctness cliff: turning a tier off or shrinking a bloom only ever costs scans, never rows.

| key | default | what it does |
|---|---|---|
| `term-index` | `KAHSHE_TERM_INDEX` | whether this column gets a term dictionary (token `match`, `_count`) |
| `gram-index` | `KAHSHE_GRAM_INDEX` | whether this column gets the exact gram layer (`contains`/`eq`/`in`/`starts-with`). Off, those queries fall back to the blooms — probabilistic, still correct |
| `bloom-fpp` | 0.01 | the bloom tier's false-positive rate. Higher is smaller and prunes less. Worth raising on dense-identifier columns where grams have little selectivity anyway and the term dictionary is doing the real work |
| `ngram` | `KAHSHE_NGRAM` | the gram size the bloom and gram tiers are cut at, 2..8. Larger thins the gram space on dense-identifier columns at the cost that a `contains` literal shorter than the size cannot prune. **Changing it changes the gram rule id**: the existing index keeps serving under its own size while the column rebuilds |
| `checkpoint-files` | 0 (off) | build at most this many new files per pass and publish each pass, so a long build that dies resumes from its last checkpoint instead of file zero. A pass that is not the last is marked partial: freshness reads it as not current and `_count` refuses it; pruning is unaffected. Each pass re-merges the ranges its files touched, so set it to a fraction of the table, not a handful |
| `analyzer` | `tokens` | what a value becomes in the term dictionary. See below |
| `max-token-length` | `KAHSHE_MAX_TOKEN_LENGTH` | longest token written into the dictionary. **Changing it changes the analyzer id**: the existing index keeps serving under its own cap (readers take the cap from the index, never from the proxy), and the indexer rebuilds the column in full on its next observation rather than waiting for the table to change, so tokens between the old and new cap are never probed for before they are indexed |
| `kahshe.plan-stats` (table-level, not under `kahshe.index`) | `KAHSHE_PLAN_STATS` | `strip` or `requested`: whether plan responses for this table carry the per-file column statistics a request names. Serving-time only; nothing in the index changes |

**The ceiling rule.** A deployment flag is a ceiling, not a default a property can exceed: a
property can turn a tier off for a column, but cannot enable one the deployment has off. The build
is budgeted and the serving caches are sized by the deployment flags, so a widening attempt logs a
WARN and does nothing.

**Values are read strictly.** A toggle must spell `true` or `false` — case-insensitive and
trimmed, so `TRUE` and ` false ` are fine — and anything else is refused with a WARN and that
level skipped, deliberately not `Boolean.parseBoolean`, which reads every typo as `false`.
Malformed numbers likewise WARN and fall through to the next level.

### `analyzer`: `tokens` or `value`

`tokens` (the default) is ASCII runs, lowercased, plus — since analyzer v3 — every IPv4/IPv6
address, UUID and dashed or underscored hex identifier the text carries, whole as well as in
pieces. `value` is the canonical value whole, exact and case-sensitive.

Use `value` for ids and addresses whose pieces every row shares: a client IP tokenizes into octets
and saturates the gram tier, so a rare IP prunes nothing under `tokens` and prunes to its files
under `value` through a plain `=` / `IN`. Changing the analyzer changes the analyzer id (a full
rebuild, the existing index served meanwhile).

On a `value` column, `_count` counts the rows holding exactly that value, and `LIKE 'x%'` is a
prefix over the dictionary — every address, id or key under the prefix, joined. So is any string
range (`>=`, `<`, `BETWEEN`; Trino pushes `LIKE 'x%'` down as one): subnet and id-prefix search
from plain SQL, no extra tokens.

A typical shape for a table with one prose column and one id column:

```sql
ALTER TABLE logs.events SET PROPERTIES (
  'kahshe.index' = 'msg,trace_id',
  'kahshe.index.trace_id.gram-index' = 'false',
  'kahshe.index.trace_id.bloom-fpp' = '0.2');
```

### `kahshe.index.scope`: indexing a window of the table

```sql
ALTER TABLE logs.events SET PROPERTIES (
  'kahshe.index.scope' = '{"type":"gt-eq","term":"ts","value":"2026-08-01T00:00:00"}');
```

The value is an Iceberg expression in the JSON form Iceberg itself serialises — the same dialect
`/plan` accepts — handed to `newScan().filter(...)`, so Iceberg prunes partitions before kahshe
opens a single data file.

What it buys: index cost is paid once per file while query value decays with age, and on a
partitioned table Iceberg's own partition pruning runs *before* kahshe's — a query filtered to one
day has already had the other days' files eliminated before the pruner sees anything, so indexing
them bought that query nothing. Aligning coverage with the partitions people actually query is
close to free; seven days of a ninety-day table is about a thirteenth of the work.

**Widening is incremental, narrowing is a rebuild.** A wider scope is a superset of what is
already covered, which is exactly the condition the incremental path tests, so only the files the
widening added are read — index seven days now and fourteen later, and the first seven are not
read again. Narrowing forces a full rebuild: the coverage list then names files the scan no
longer returns, the incremental gate fails, and the index is rebuilt over the narrower set.

A malformed expression **fails the build loudly** rather than defaulting to the whole table or to
nothing. Both defaults are safe in the advisory-keep sense, which is precisely why neither would
be noticed. The scope in force is recorded in the term index metadata (`properties.index-scope`)
so that "40 of 900 files covered" reads as a decision rather than a failure.

---


## TLS

Off unless configured, which keeps the quickstart working on a loopback socket. **Port 8282
otherwise carries bearer tokens in the clear**, so anything beyond a trust boundary you already
control wants either these settings or a TLS-terminating proxy in front.

Supply a certificate one way or the other; setting both is refused at startup rather than ranked.

| Variable | Default | Effect |
| --- | --- | --- |
| `KAHSHE_TLS_CERT`, `KAHSHE_TLS_KEY` | — | PEM certificate chain and its PKCS#8 private key. This is what a `kubernetes.io/tls` Secret holds (`tls.crt`, `tls.key`), so nothing needs converting. A PKCS#1 key is refused with the `openssl pkcs8` command that converts it |
| `KAHSHE_TLS_KEYSTORE`, `KAHSHE_TLS_KEYSTORE_PASSWORD` | — | A keystore holding both, for a deployment that already has one |
| `KAHSHE_TLS_KEYSTORE_TYPE` | `PKCS12` | `PKCS12` or `JKS` |
| `KAHSHE_TLS_CLIENT_AUTH` | `none` | `none`, `want` or `need` — mutual TLS. Anything but `none` requires `KAHSHE_TLS_CLIENT_CA` |
| `KAHSHE_TLS_CLIENT_CA` | — | PEM bundle of the CA that must have signed a client certificate |
| `KAHSHE_TLS_ADMIN` | `false` | Serve the admin port over TLS too. Off because a collector scraping inside the pod usually wants plaintext |
| `KAHSHE_TLS_RELOAD_MS` | `60000` | How often the certificate files are re-checked for a rotation |

**Certificates are re-read while the process runs.** A proxy that loads its certificate once
serves an expired one the day it is renewed, which is a 3 a.m. failure with a 90-day fuse. A
changed file takes effect on the next handshake with no restart; one that fails to load leaves
the previous certificate serving rather than dropping TLS.

TLS 1.3 and 1.2 are offered; nothing below that.

### Trusting the backend

The other direction: a backing catalog whose certificate comes from a **private** authority, which
the JVM's default trust store does not know.

| Variable | Default | Effect |
| --- | --- | --- |
| `KAHSHE_BACKEND_CA` | — | PEM bundle of the CA that signed the backing catalog's certificate |

kahshe talks to the backend through two clients — Iceberg's `RESTCatalog`, which wraps Apache
HttpClient 5, and the passthrough forwarder, which uses the JDK's — and this configures both. It
is **scoped to those clients**: the S3 client that reads and writes index files keeps the JVM's
default trust, which a `-Djavax.net.ssl.trustStore` would not have left alone.

The bundle **replaces** rather than augments the default for those connections. A private CA is
normally the only authority that should be trusted on that hop, and also trusting every public
root would make a misissued public certificate indistinguishable from the intended one. Hostname
verification stays on.


## 2. Environment variables

### Serving and the backing catalog

| var | default | purpose |
|---|---|---|
| `KAHSHE_PORT` | 8282 | listen port (data plane) |
| `KAHSHE_ADMIN_PORT` | 8283 | health/readiness/metrics port, on its own executor, so probes stay alive during backend brownouts |
| `KAHSHE_BACKEND` | `http://localhost:8181/api/catalog` | backing REST catalog base |
| `KAHSHE_CREDENTIAL` | — | kahshe's own client-credentials for planning reads |
| `KAHSHE_SCOPE` | `PRINCIPAL_ROLE:ALL` | OAuth scope for the above |
| `KAHSHE_BACKEND_WAREHOUSE` | — | backend warehouse name when the URL prefix is not one (Nessie's `branch\|warehouse` prefixes) |
| `KAHSHE_BACKEND_TIMEOUT_MS` | 5000 | per-request timeout toward the backing catalog |
| `KAHSHE_INJECT_PLANNING` | true | inject `scan-planning-mode=server` on `loadTable`. **`false` turns off more than the injection**: the same branch is where the indexer observes `kahshe.index` in passing traffic and where the plan path records the snapshot this replica forwarded, so automatic maintenance and the stale-view guard go with it. Nothing errors — indexes simply stop building on their own. What is left is rule-driven: `TableDiscovery` observes the same property, but only for tables a watch rule names, only where the watch role runs (`KAHSHE_WATCH_RULES` set and `KAHSHE_MODE` `watch` or `both` — a `proxy`-mode instance has nothing left), and only with `KAHSHE_INDEXER=true`; otherwise the `kahshe index` CLI backfill is the only way a column gets built |
| `KAHSHE_WORKER_THREADS` | 32 | data-plane pool size (bounded queue of 256 behind it) |
| `KAHSHE_MAX_BODY_BYTES` | 16 MiB | request body cap (413 above it) |
| `KAHSHE_MODE` | both | `proxy`, `watch`, or `both`. See [OPERATIONS.md](OPERATIONS.md#4-deployment-shapes) |

### Authorization and disclosure

| var | default | purpose |
|---|---|---|
| `KAHSHE_AUTH_CACHE_TTL_MS` | 60000 | caller-authorization cache TTL, capped at token expiry |
| `KAHSHE_PLANNING_IDENTITY` | service | `caller` plans and counts under the caller's own token, so backend authorization applies to every metadata read |
| `KAHSHE_PLAN_STATS` | strip | whether a plan response carries per-file column statistics: `strip` returns none, `requested` returns those of the columns the request names in `stats-fields` and no others. A table overrides it with `kahshe.plan-stats`. Disclosure traded for scan speed: without them Trino decodes every page of every kept file, ~1.3 ms per file on the lab cluster, which only shows when nothing prunes |

### Correctness and caching

| var | default | purpose |
|---|---|---|
| `KAHSHE_TABLE_CACHE_TTL_MS` | 10000 | how long a loaded table may be reused before it is re-read. **Set to `0` when running more than one replica** — the record of which snapshot this proxy last forwarded is per-process, so a replica that did not serve a table's `loadTable` can otherwise plan from a view of unknown age. Costs one metadata read per plan (~6 ms against Nessie) |
| `KAHSHE_SERVE_DELETE_BEARING` | false | serve server-side plans for snapshots carrying delete files. **Leave false unless no Trino reads through this proxy**: Trino 483 unboxes the sequence number the REST scan-task format drops and throws, while stock iceberg-java readers are correct (measured, both delete kinds) |
| `KAHSHE_CACHE_BYTES` | auto: 40% of max heap | total in-memory cache budget, split 40/25/20/15 across bloom/term/plan/gram caches, 8 MiB floors |
| `KAHSHE_PREFIX_MAX_TERMS` | 100000 | the most dictionary terms a prefix query (`LIKE 'x%'` on a whole-value column, a `match_prefix` hint, a `_count` by prefix) may union before the proxy keeps every file instead, loudly (`kahshe_term_prefix_capped_total`). Bounds what one short prefix can cost |

### Index storage

| var | default | purpose |
|---|---|---|
| `KAHSHE_INDEX_ROOT` | `<table location>/_index` | index storage root. **Move it outside the table location when table maintenance runs `remove_orphan_files`** — index files are not referenced by table metadata and would be deleted. Indexes then land under `<root>/<table-uuid>-<location-hash>`, the hash keeping diverged `registerTable` clones that share a uuid from rebuilding over each other |
| `KAHSHE_INDEX_IO_IMPL` | `org.apache.iceberg.aws.s3.S3FileIO` for an `s3` root, otherwise the table's own IO | the `FileIO` implementation class for the index store, resolved exactly as Iceberg resolves one (`CatalogUtil.loadFileIO`). Any `FileIO` on the classpath works, so a GCS or ADLS index root is configuration rather than code. The class must be on the runtime classpath — the distribution ships iceberg-aws, anything else is an added dependency |
| `KAHSHE_INDEX_IO_PROPERTIES` | — | that implementation's own configuration: comma-separated `key=value` pairs using Iceberg's dotted keys, e.g. `s3.endpoint=http://minio:9000,s3.path-style-access=true`. Keys and values are trimmed, empty entries skipped, and only the first `=` splits an entry, so a value may carry its own |
| `KAHSHE_INDEX_S3_ENDPOINT`, `KAHSHE_INDEX_S3_ACCESS_KEY`, `KAHSHE_INDEX_S3_SECRET_KEY` | — | shorthand for four of those keys, for an external `s3://` index root — needed when the catalog vends table-scoped credentials or remote signing, since signers refuse URIs outside the table location. All optional: leave the keys unset and the S3 client uses the AWS default credential provider chain (IRSA / workload identity / instance profile / environment), which is the only one that rotates without a restart. Leave the endpoint unset for AWS itself; set it for MinIO, Ceph and the like. An entry in `KAHSHE_INDEX_IO_PROPERTIES` wins over the shorthand for the same key |
| `KAHSHE_INDEX_S3_REGION` | us-east-1 | region for the above |
| `KAHSHE_DATA_IO` | `table` | who reads data files during a build: `table` uses the table's own FileIO, `index` routes them through the index client's credentials |

### Building

| var | default | purpose |
|---|---|---|
| `KAHSHE_INDEXER` | true | automatic index maintenance driven by the `kahshe.index` table property |
| `KAHSHE_INDEX_THREADS` | 8 | concurrent data-file readers per index build |
| `KAHSHE_TERM_BUFFER_BYTES` | 64 MiB | the byte arena one reader fills before sorting it and writing a run. Peak term-tier heap is this × `min(KAHSHE_INDEX_THREADS, files)`, **allocated** up front rather than grown, so raising it costs that memory immediately |
| `KAHSHE_TERM_BUILD_DIR` | `<java.io.tmpdir>/kahshe-term-build` | where those sorted runs live. One directory per build, removed on success and on failure alike |
| `KAHSHE_TERM_BUILD_MAX_SPILL_BYTES` | 16 GiB | local-disk budget for those runs, counted as each run closes. **Every build that writes the term tier writes runs** — there is no in-heap mode to fall back to — so wherever the tier is on the volume is required rather than conditional, and such a build probes the directory before reading a single data file. Where the tier is off for every column (`KAHSHE_TERM_INDEX=false`, or `term-index=false` on each), no run is written, the directory is never probed, and no local volume is needed. Crossing the budget fails the build naming both numbers, rather than publishing a term index missing terms, which would prune files that contain them. Runs are not coalesced across data files before the merge, so this wants more headroom than the aggregate's own size suggests |
| `KAHSHE_TERM_INDEX` | true | build and serve the term dictionary. `false` builds blooms and grams only — substring pruning is unaffected and `match` predicates keep every file. Turning it back on forces a full rebuild: coverage records which *files* were read, not which tiers were written from them |
| `KAHSHE_GRAM_INDEX` | true | build and serve the exact gram layer; `false` is bloom-only behavior end to end |
| `KAHSHE_GRAM_BUILD_MAX_BYTES` | 1 GiB | build-memory cap for the in-progress gram map, checked before the first file and after each wave of `KAHSHE_INDEX_THREADS` files; past it the build ships no gram layer and the table serves from blooms |
| `KAHSHE_NGRAM` | 3 | default gram size for the bloom and gram tiers, 2..8. Changing it changes the gram rule id, so affected columns rebuild rolling |
| `KAHSHE_MAX_TOKEN_LENGTH` | 256 | longest token written into a term dictionary. Follows Elasticsearch's `ignore_above` convention: a valve against pathological tokens, not a filter on ordinary content |
| `KAHSHE_INDEX_STALE_WARN_MS` | 300000 | how long a table may sit with an observed-but-unbuilt snapshot before a WARN, rate-limited to one per table per interval. Also the threshold to alert on via `kahshe_index_max_behind_seconds` |
| `KAHSHE_INDEX_FLEET` | false | this process is one of SEVERAL builders sharing the work. A column another member holds the build lease on is then skipped and counted (`kahshe_index_lease_skips_total`), the table left unclaimed so a later poll revisits it, and the claim order rotated so N members start on N different columns. Off, a held lease is a loud refusal, which is right when nothing else is going to build that column |
| `KAHSHE_INDEX_FLEET_ORDINAL` | — | this member's claim order. Unset derives one from the pod name — a StatefulSet's `name-N` gives its ordinal, any other name a stable hash. It is a spread, not an assignment: two members on the same ordinal cost a skip, never a wrong answer |

### Build-memory ceilings (JVM system properties)

Two caps are read as JVM system properties, so they are set with `-D…` on the command line rather
than exported. Despite the `kahshe.` prefix they are **not** table properties: `ALTER TABLE … SET
PROPERTIES` on either does nothing.

| property | default | purpose |
|---|---|---|
| `kahshe.bloom.build.max.bytes` | 1 GiB | the accumulated per-file bloom map, which grows with the table's file count because every file's bloom is held until the leaf is written. Past it the build **refuses**, naming the MiB and the file count and offering the cap, `kahshe.index.scope` or compaction; the existing index is untouched and still correct. Refusing rather than degrading, unlike the gram valve: dropping the blooms mid-build would leave the leaf list and coverage disagreeing about what was written |
| `kahshe.watch.max.distinct.terms` | 16000000 | distinct terms in the per-file term map a watch token rule reads, about 400 MiB of keys and counts at the cap. A file past it stops accumulating and says so; token (`match`) rules are then **skipped** for that file with a WARN and `kahshe_watch_counts_truncated_total`, while `contains` rules read the uncapped gram set and are unaffected |

### Non-REST catalogs (watch role only)

| var | default | purpose |
|---|---|---|
| `KAHSHE_CATALOG_IMPL` | — | any Iceberg `Catalog` class on the classpath — Glue, Hive, JDBC, DynamoDB — loaded exactly as Iceberg loads one (`CatalogUtil.loadCatalog`), so a watcher runs on a lakehouse kahshe has never seen without code. **Ignored, with a WARN, in `proxy` or `both` mode**: the proxy *is* a REST catalog and forwards what it does not serve, so a non-REST backend would half work. A rule's `prefix` is a REST tenancy segment, not a client selector — one catalog serves every rule, and several tenants means several watchers |
| `KAHSHE_CATALOG_PROPERTIES` | — | that catalog's own configuration, comma-separated `key=value` pairs using Iceberg's dotted keys, parsed exactly as `KAHSHE_INDEX_IO_PROPERTIES` is. `KAHSHE_BACKEND_WAREHOUSE` fills in the warehouse when these do not name one; an entry here wins |

### Watch

| var | default | purpose |
|---|---|---|
| `KAHSHE_WATCH_RULES` | — | path to the rules YAML (see `../helm/kahshe/examples/watch-rules.yaml`); hot-reloaded on mtime change |
| `KAHSHE_WATCH_SINK` | webhook | which `AlertSink` delivers: `webhook`, `log` (one INFO line per alert carrying the payload JSON — dev stacks, no receiver needed), or `none` (delivers nothing, deliberately: fleet members write build reports and one watcher delivers from them, keeping dedup in a single process). Alerts still count and still log under `none`. Any provider registered under `META-INF/services/io.kahshe.watch.sink.AlertSinkProvider` is selectable by name; an unknown name **fails startup** with the discovered names listed, rather than falling back |
| `KAHSHE_WATCH_WEBHOOK` | — | alert webhook URL (POST JSON); validated at startup — malformed config disables the webhook with an ERROR log, alerts still log and count |
| `KAHSHE_WATCH_WEBHOOK_AUTH` | — | literal `Authorization` header value for the webhook. Keep it in a Secret |
| `KAHSHE_WATCH_WEBHOOK_TIMEOUT_MS` | 5000 | per-attempt webhook timeout (3 attempts, 1s/5s backoff, bounded queue of 1024 — full = drop + count) |
| `KAHSHE_WATCH_POLL_MS` | 60000 | discovery poll interval over the tables named in rules |
| `KAHSHE_WATCH_SQL_CATALOG` | iceberg | catalog name used in emitted `confirmation_sql` |
| `KAHSHE_WATCH_SCAN` | true | the row scan. `false` leaves only the index-riding path, which costs no data read but can answer only single-column `match`/`contains` rules on indexed columns, unwindowed, as one field or an `any-of` — an `all-of` over two tokens or fields, a window rule and every other operator then never fire (WATCH.md §1) |
| `KAHSHE_WATCH_SCAN_THREADS` | 4 | how many data files the row scan reads at once. Each thread holds one projected Parquet reader; the read is of the named columns only, never the whole row |
| `KAHSHE_WATCH_REALERT_ON_REBUILD` | false | `true` re-enables alerts for previously covered files during full rebuilds (suppressed by default) |
| `KAHSHE_WATCH_REPLAY_MAX_FILES` | 2000 | the most files ONE table's first-sight replay may read to rebuild the state a restart lost. Past the budget it replays the most recent files, WARNs with the three ways out, and counts `kahshe_watch_replay_truncated_total` — what it did not rebuild is a window that will not fire |
| `KAHSHE_WATCH_WINDOW_MAX_KEYS` | 200000 | the most live keys ONE window rule may track before the least recently seen are evicted, ~186 bytes each at `count: 5`. An eviction throws away a partial window, which is a **missed detection**, so it is counted (`kahshe_watch_window_key_evictions_total`) rather than absorbed |

---

## Further reading

- [ENDPOINTS.md](ENDPOINTS.md) — the served HTTP surface and filter extensions
- [OPERATIONS.md](OPERATIONS.md) — metrics, staleness alerting, index lifecycle, deployment shapes
- [WATCH.md](WATCH.md) — the rule language
- [ARCHITECTURE.md](ARCHITECTURE.md) — why the knobs are shaped this way
