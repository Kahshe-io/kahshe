# kahshe architecture

This document is for someone who has never seen the codebase. It explains what kahshe is, how a
request moves through it, what the indexer builds, what the format stores, what the watcher does,
and the one invariant that makes all of it safe to run.

For the bytes on disk, see [FORMAT.md](FORMAT.md). For build, test and contribution mechanics, see
[../CONTRIBUTING.md](../CONTRIBUTING.md). Each module has its own README, linked from the map at
the end.

---

## 1. What kahshe is

kahshe is a proxy that sits between query engines and an Iceberg REST catalog.

```
engines ──REST──▶ kahshe ──▶ your existing catalog
                    │
                    └─ serves /plan (server-side scan planning), pruning data
                       files using Parquet sidecar indexes
```

An engine sends its filter, the catalog answers with the list of files to scan. Iceberg's min/max
statistics prune that list when the predicate has bounds — and cannot touch `LIKE '%needle%'`, a
regexp, or a value whose range every file spans. Those queries read the whole table.

Iceberg's REST specification already has the hook: server-side scan planning (`planTableScan`).
kahshe implements those endpoints, advertises them in `/v1/config`, and injects
`scan-planning-mode=server` into `LoadTableResponse`, so stock Iceberg 1.11+ clients switch over
with no configuration. Planning consults three Parquet sidecar tiers — character-gram blooms, an
exact gram layer, a term dictionary (section 5) — and returns only the files that can match.
Everything kahshe does not serve itself is forwarded to the real catalog unchanged. The lineage is
ClickHouse-style data skipping (`ngrambf_v1`) brought to open Iceberg tables;
[diagrams/](diagrams/DIAGRAMS-README.md) draws it on a ten-file table.

**kahshe returns file references. It never returns rows and never reads a data file on any serving
path.** There is no ranking, no scoring, no row retrieval. Pruning is the entire product.

Indexes are ordinary Parquet files living beside the data in the same object store, keyed by
Iceberg **field id** and never by column name. They are rebuildable from the table at any time, so
they are cache, not truth. A
table opts in with a property:

```sql
ALTER TABLE logs.events SET PROPERTIES ('kahshe.index' = 'msg');
```

That is the entire configuration. There is no index DDL, no index service, no new table format,
and nothing for the engine to know about.

Removal is the same size. Point the catalog URI back at your catalog and that is the whole of it:
no metadata to unwind, no format to convert, no data to migrate, no engine change to revert because
there never was one. What is left is an `_index` directory of Parquet files your table metadata
never referenced and an inert `kahshe.index` property.

---

## 2. The invariant

Read this before anything else. Every design decision downstream of it only makes sense in its
light.

> **kahshe may cause a data file to be SKIPPED only when that file certainly cannot match. On any
> error, doubt, partial state, or staleness, it KEEPS the file.**

Two error directions, and they are not symmetric:

- **False positive** — keeping a file that turns out not to match. It costs one scan and no rows:
  the engine re-applies its own filter to what it reads, as in any Iceberg scan. This happens
  constantly and is perfectly acceptable. Do not "fix" it at any risk to the other direction.
- **False negative** — pruning a file that *does* match. It silently deletes rows from a user's
  query result. There is no exception, no error, and no metric when it happens. This is the one
  error class the system may never have.

### What the invariant buys

A file the index does not know about is KEPT. So an index that is incomplete, stale, partially
built, unreadable, or entirely absent costs speed and never correctness.

Most index systems must be COMPLETE to be CORRECT: a document missing from the index is a document
missing from results. kahshe runs the other way, and that changes what is possible operationally:

- An index can cover a **window** of a table (`kahshe.index.scope`) and still return exactly the
  rows a complete index would. Coverage is a cost/benefit dial with no correctness cliff.
- A tier can be turned off, a bloom shrunk, a build deferred — all cost dials.
- A build that dies mid-flight leaves the previous generation serving, and the table is correct
  the whole time.
- Rebuilding from scratch is always a valid recovery.

### The trap the invariant sets

Because the invariant absorbs errors, **every failure in this system looks identical to "the data
was less selective than you thought."** A broken index, a stale index, an unreadable artifact, a
disabled tier and a genuinely unprunable query all produce the same observable: more files
scanned. Two consequences shape the code:

- **Operator mistakes are made loud on purpose.** A malformed `kahshe.index.scope` fails the build
  rather than defaulting to everything or nothing — both of those defaults are *safe*, which is
  exactly why neither would ever be noticed.
- **Anything the invariant would otherwise hide gets a log line or a metric.** "It is still
  correct" is not a reason to be quiet. This is why staleness is a gauge
  (`kahshe_index_max_behind_seconds`) rather than a silent condition, and why a build that finds
  another builder's lease says so by name.

The single most important branch in the read path is in
`format/src/main/java/io/kahshe/format/IndexPruner.java`: the catch that turns "I could not read
this artifact" into "keep every file", rather than into the far more natural and catastrophic "the
term is absent, so prune." Every tier reader applies the same rule to a leaf it refuses.

---

## 3. The request path

```
  engine
    │  Iceberg REST over HTTP or HTTPS, port 8282
    ▼
  KahsheHandler ──── dispatch, request body cap, cache invalidation on commit
    │
    ├─▶ AuthGate ─── per-request caller authorization: replays loadTable as the
    │                caller, so kahshe never widens anyone's access
    │
    ├─▶ Forwarder ─── everything kahshe does not serve: header-filtered
    │                 passthrough to the backing catalog
    │
    ├─▶ Mutations ─── /v1/config gains the plan endpoints; LoadTableResponse
    │                 gains scan-planning-mode=server
    │
    ├─▶ PlanRoutes ── /plan, plan-status, tasks, cancel; error mapping
    │     │
    │     ▼
    │   PlanService ─ plan cache, metrics, delete-bearing check
    │     │
    │     ├─▶ ContainsExtractor ─ strips contains / match / apply / sentinel
    │     │                       predicates out of the filter into hints
    │     ▼
    │   IndexPruner ─ each tier, in cost order, narrows what the last one kept;
    │     │           a tier that cannot answer passes it through
    │     │
    │     ▼
    │   file-scan-tasks ─────────────────────────────▶ back to the engine
    │
    └─▶ CountRoutes ─ /_count from the aggregate term tier, or a refusal

  AdminHandler, port 8283 ── /healthz /readyz /metrics
```

### Step by step

**Transport.** `ServerTls` builds the HTTPS server when `KAHSHE_TLS_CERT`/`KAHSHE_TLS_KEY` — or
`KAHSHE_TLS_KEYSTORE`, and supplying both is refused by name — are set, and the admin port too
under `KAHSHE_TLS_ADMIN`. It is constructed before either port binds, so a bad certificate fails
startup rather than the first handshake. Mutual TLS is `KAHSHE_TLS_CLIENT_AUTH` with
`KAHSHE_TLS_CLIENT_CA`, and the files are re-stat'ed at most once per `KAHSHE_TLS_RELOAD_MS` so a
cert-manager renewal takes effect on the next handshake with no restart; a rotated file that fails
to load leaves the previous context serving rather than dropping TLS. Unset, both ports are
plaintext and a WARN says so. `BackendTls` is the same concern in the other direction: a private CA
for the backing catalog (`KAHSHE_BACKEND_CA`), handed to both clients that reach it — Iceberg's
`RESTCatalog` through the one seam 1.11.0 offers, and the passthrough `Forwarder` through the JDK
client — rather than installed as the JVM default, which would also change how the S3 client that
writes index files trusts object storage.

**Dispatch.** `KahsheHandler` is the data plane. It caps request bodies, routes, and — when a
commit passes through — invalidates the caches for the tables that commit touched.
`MutatedTables` works out which tables those are, but only when the request body names them; when
it does not, the conservative action is taken instead.

**Authorization.** `AuthGate` authorizes by replaying `loadTable` against the backing catalog as
the *caller*, not as kahshe's own service identity. kahshe therefore cannot serve a plan for a
table the caller could not have loaded themselves. The verdict is memoized rather than re-asked on
every request: the key is a SHA-256 of the token plus the table, never the token, and it lives for
`KAHSHE_AUTH_CACHE_TTL_MS` (default 60 s) or until the token's own JWT `exp`, whichever is sooner —
so revocation lags by at most that TTL. A 401/403/404 is cached for 2 s so a bad-token storm does
not amplify 1:1 into the backend; a 5xx is cached for nothing, because "the backend could not
answer" is not an authorization answer. `BackendCatalogs` holds both identities and keeps them
apart.

**Advertisement.** `Mutations` rewrites two responses: `/v1/config` to advertise the plan
endpoints, and `LoadTableResponse` to set `scan-planning-mode=server`. That pair is why a stock
client flips to server-side planning with zero configuration — the client is following the spec,
not a kahshe extension. Both rewrites fail open: a response that cannot be rewritten passes through
unmodified, so the client sees its catalog and plans locally, and
`kahshe_response_rewrite_failures_total` counts it — the only other symptom would be queries that
stop getting faster.

**Filter extraction.** The engine's filter arrives as an ordinary Iceberg expression. Before
planning, `ContainsExtractor` pulls out the predicates only kahshe can answer — `contains`,
`match`, `match_prefix`, the merged expressions spec's `apply` form, and the sentinel described in
section 7 — and turns them into hints. Everything else stays in the filter and is answered by
Iceberg's own statistics pruning. Extensions are accepted in **conjunctive positions only**: under
`OR` or `NOT` they are a 400, never a silent pass, because a silently dropped predicate is a
correctness question dressed as a convenience.

**Pruning.** `IndexPruner` is the read path. It receives the file list after Iceberg's own
partition and min/max pruning has run on it, so a query whose whole predicate is one day of a
partitioned table gains nothing from the tiers, and a full scan has nothing to prune. For each hint
it consults the tiers that can answer it and produces the set of files to keep. Anything it cannot
answer — an unreadable leaf, a file outside the index's coverage, a tier that is not built —
resolves to *keep*.

**Planning.** `PlanService` assembles the surviving files into `file-scan-tasks`, caches plans,
and records metrics. It also declines to serve server-side plans for snapshots that carry delete
files unless explicitly opted in (`KAHSHE_SERVE_DELETE_BEARING`), and `Mutations` withholds
`scan-planning-mode=server` from the same snapshots under the same flag — so on a delete-bearing
snapshot server planning is neither advertised nor served, and the client plans for itself, which
is correct and merely slower.

**Counting.** `CountRoutes` answers `/_count` from the aggregate term tier — exact token counts by
term or by prefix. It **refuses rather than approximates**: deletes present, coverage stale, a
multi-token value, or any data file having left the table since the index was built all produce a
refusal rather than a number that is nearly right. Pruning is unaffected by that last case,
because pruning only asks *which* files hold a term, never how many times.

**Admin.** `AdminHandler` serves `/healthz`, `/readyz` and `/metrics` on a separate port with its
own executor, so probes answer while the data plane is saturated. `/readyz` reports the last
verdict of a backend probe that re-observes the backing catalog every 5 s, so an unreachable
catalog turns it false within seconds. That probe runs off the request thread and must stay off
it: the handler runs on a single-threaded executor, and a probe on the request thread turns a slow
backend into an unanswerable health check.

---

## 4. What the indexer builds

The indexer is maintenance, not a service call. Nobody asks for an index; the property asks, and
the indexer notices.

**Discovery.** `IndexerService` observes the `kahshe.index` property on tables in passing
`loadTable` traffic. Commits through the proxy mark a column's index stale, and the next
observation triggers a rebuild. `IndexFreshness` keeps per-table staleness gauges so a maintenance
loop that has quietly stopped is visible — remember that a stale index produces no wrong answers,
so nothing else would ever report it.

**The one catalog seam.** The indexer reaches a catalog only through `TableSource`: load a table
by prefix and identifier, invalidate one. `BackendCatalogs` implements it over REST for the proxy;
a watcher-only deployment can load any Iceberg `Catalog` by class name instead. Nothing else in
the indexer knows what a catalog is.

**Settings resolution.** `IndexSettings` resolves every knob per column
(`kahshe.index.<column>.<key>`), then per table (`kahshe.index.<key>`), then from the deployment
default. Deployment tier flags are a **ceiling, never a default**: a table property can turn a
tier off for a column, but cannot enable one the deployment has off, because the build is budgeted
and the serving caches are sized by the deployment flags. A widening attempt logs a warning and
does nothing.

**Budget.** `BuildBudget` refuses a build whose configured knobs cannot fit the heap, before the
build starts, rather than discovering it at hour three.

**The lease.** `BuildLease` writes a lease file beside the column's index metadata for the
duration of a build, so one column has one builder. A second builder — another replica, or a CLI
run beside the running indexer — refuses loudly and names the holder. A JVM shutdown hook releases
every lease the process holds; the TTL exists only for a process that died without running its
hooks.

**The build.** `IndexBuilder` does the read pass over the table's data files, accumulates each
tier's structures in waves, spills sorted runs to disk when the term build exceeds its memory, and
merges them. It then decides between three publishes:

- a **full build**, writing fresh artifacts;
- a **restamp**, when no data file changed and only the metadata needs to name a new snapshot;
- a **compacting build**, when enough files have left the table that coverage is worth
  renumbering.

Those three are not interchangeable, and section 6 explains why the compaction case is the one to
be careful with.

**Reporting.** Each build writes a `BuildReport` beside the term metadata: what it covered, what
it skipped and why, and any alerts raised along the way. That is how a watcher elsewhere learns
what a build somewhere else saw.

---

## 5. What the format stores

`format/` owns the artifact — every tier's reader *and* writer together, so the two cannot drift
apart in separate modules. [FORMAT.md](FORMAT.md) specifies it normatively; this is the shape.

### Three tiers, all keyed by field id

| Tier | Artifact | Answers |
|---|---|---|
| n-gram bloom, per data file | `ngram-bloom-f<id>/leaf-<snap>-<nonce>.parquet` | `contains`, for files the gram tier does not cover |
| exact gram → file bitmap | `term-v1-f<id>/grams-<snap>-<nonce>.parquet` | `contains`, `eq`, `in`, `starts-with` — exactly |
| term dictionary, range-partitioned | `term-v1-f<id>/aggregate-<snap>-r<NN>-<nonce>.parquet` | `match` (token equality), `match_prefix`, `_count`; on a whole-value column also `eq`/`in`, `starts-with` and string ranges, as a dictionary range scan |

Keying on **field id** and not column name is why renaming an indexed column is safe.

The tiers degrade into each other in one direction only. Drop the gram tier and `contains` falls
back to the blooms — probabilistic, slower, still correct. There is no equivalent fallback for a
term aggregate that is missing terms, which is why a term build that cannot complete **fails
loudly** instead of shipping something partial: a dictionary missing a term prunes exactly the
files that contain it.

Identifier-dense text defeats the gram tiers outright: there are only 4,096 possible hexadecimal
trigrams, so essentially every file of hex trace ids holds all of them. On a lab corpus of 48 GB
and 2.1 billion rows of that shape, `contains` kept **220 of 220 files** for the most selective
query it can pose — a run predating the evidence records in the README's
[measured results](../README.md#measured), so read it as the reason the term dictionary is not
optional for such a workload rather than as a headline figure. That is a limit of the approach,
not of this implementation; the build counts those files on `kahshe_gram_saturated_files_total`.

### Coverage is the join key

`Coverage` maps data files to **allocated ordinals**, plus tombstones for files that have left the
table. Bitmaps in the gram and term tiers are sets of those ordinals.

Ordinals are *allocated*, not positional. Dead entries keep their slots forever. If coverage ever
renumbers while a tier's bitmaps do not — or the reverse — a live lookup resolves to a different
file's data, and that is a false negative with no symptom. Every compaction path in the codebase
exists to keep those two in step.

Coverage is also how the invariant is enforced structurally: a data file with no ordinal is not
"absent from the index", it is **outside coverage**, and outside coverage means keep.

### The analysis layer

`analysis/` sits *below* the artifact and depends on nothing of kahshe's at all. It holds the two
things a reader and a writer must agree on whether or not an index exists:

- **`Canonical`** — the one string form of a value. Integers as decimal text, decimals as plain
  text with trailing zeros stripped, UUIDs as 32 undashed hex digits, binary as lowercase hex. The
  build writes this form and the pruner probes with it, through the same method, because a literal
  canonicalised any other way finds its own value absent and prunes the file that holds it.
- **`Analyzer`** — what a token is. Maximal ASCII `[a-z0-9]` runs after locale-independent
  lowercasing, plus whole compound identifiers (IPv4, IPv6, UUID, dashed or underscored hex). A
  whole-value analyzer instead appends each canonical value as one term — one per row on a scalar
  column, one per distinct member on a list or map, whose members are canonicalised on their own
  and never joined — for columns an operator declares to be identifiers.

Both are **pinned contracts** carried in the index metadata as an analyzer id that includes the
family and the token-length cap. The build tokenizes data with the analyzer and the query
tokenizes predicates with it; changing behaviour without minting a new id desynchronises every
existing index from every new query, and no self-consistency test can notice. The reader refuses
any id outside the family it can read, and older families are read under their own contracts while
maintenance rebuilds them.

This is also why `analysis` is a module of its own: a row-scan-only watcher needs these rules and
not the artifact, and a second implementation in another language can read the module as a
specification.

### Isolation

`format` depends on `common`, `analysis` and Iceberg's read/write libraries — never on the
indexer, the proxy, the watch, or a storage client. The application registers external index IO
through `IndexPaths.externalIo`, which is why an index root on a different object store is
configuration rather than code.

`format/src/test/resources/conformance/v1/` holds a frozen golden artifact with its corpus and
manifest, read by conformance tests for the reader, the writer and the analyzer. It is what a
second implementation would be tested against.

---

## 6. Mechanisms the type system does not enforce

These are the places where a reasonable-looking edit produces a silent false negative. Read the
relevant row before editing the named file.

| Mechanism | Where | What breaks |
|---|---|---|
| **Ordinals are allocated, not positional** | `Coverage` | A bitmap is a set of ordinals. If coverage renumbers and the bitmaps do not, or the reverse, a live lookup resolves to another file's data. Dead entries keep their slots forever. |
| **A range is carried forward by reference only on a non-compacting build** | `TermIndexWriter.finish` | Copy-forward keeps a prior range leaf's path when no run holds a row for it. A compacting build renumbers ordinals, so a carried-forward leaf would name the old numbering — copy-forward is disabled for every range whenever a remap is present. |
| **Compaction must translate every bitmap in the same build** | `IndexBuilder` → `TermIndexWriter.finish`, plus the gram map | Renumbering coverage while leaving one tier's bitmaps untranslated is a silent false negative. Blooms are exempt because they are keyed by path. A compacting build must also skip the restamp shortcut, which rewrites only metadata — going through it would publish renumbered coverage against untranslated bitmaps. The reachable case is the most ordinary retention shape there is: files aged out, nothing new ingested. |
| **`counts-exact` is sticky, and is NOT "has tombstones"** | written in `TermIndexWriter.finish`, read in `TermIndex.load`, enforced in `CountRoutes` | Compaction erases tombstones but cannot subtract a departed file's occurrences from a scalar total. A flag derived from tombstones would flip back to exact and report an upper bound labelled `"exact": true`. |
| **Aggregate leaves are positional against the range list** | `TermIndexWriter.finish`, `TermIndex.entriesFor` | Every range gets a slot, empty ones as `""`. A row must be skipped *before* its appender is opened: opening records a path, and Parquet writes no object for a writer that never receives a row, so the metadata would name a file that does not exist. |
| **A term array handed out by a cursor belongs to the row, not the cursor** | `TermRun.Cursor.next`, relied on by `RunMerger` | The merge holds the smallest term while draining every cursor carrying it, including that one. Refilling one buffer instead of allocating per row — the obvious optimization, and it looks free — changes the held term mid-drain, so rows are emitted under the wrong term. |
| **The bloom leaf list is referenced, not orphaned** | `BloomLeaf.write`, `IndexStore.load` | Every leaf in the list is opened on a cold read, so no GC may delete one. The list shrinks only by build-side compaction, which orphans old leaves rather than deleting them inline — and kahshe never reclaims them. Collection is a separate operator step, specified for a conforming collector in [FORMAT.md §8.5](FORMAT.md#85-collection): referenced = every path under `snapshots[0].leaves` and `leaf-files`, delete only unreferenced objects past a grace period. |

### Build-time constants worth knowing

Compiled in, not environment variables.

| Constant | Value | Governs |
|---|---|---|
| `BLOOM_MAX_LEAVES` | 8 | when a build rewrites the bloom leaf list into one |
| `TOMBSTONE_COMPACT_DEAD_PERCENT` | 25 | dead share past which coverage is renumbered |
| `TOMBSTONE_COMPACT_MIN_DEAD` | 16 | **and** the absolute dead count required — a fraction alone makes a three-file table renumber on its first removal |
| `TermRanges.COUNT` | 36 | aggregate range leaves; the reader refuses a list that is neither this nor 1 |
| `Grams.DEFAULT_SIZE` | 3 | default gram size, per column via `ngram`, recorded in the rule id |
| `BuildBudget.HEAP_FRACTION` | 0.8 | share of the heap the configured knobs may claim before a build is refused |
| `TermRun` buffer | 64 KiB | block size both sides of the run format buffer into |
| `BuildLease.TTL_MS` | 12 h | age past which another builder treats a lease as abandoned |

---

## 7. How a token predicate reaches the index

`contains` and `match` have a natural JSON spelling that kahshe's own `/plan` endpoint accepts
directly. But an engine that builds its filter through the stock Iceberg library cannot *emit*
that spelling, and the reason is worth understanding before proposing an alternative.

Iceberg's `Expression.Operation` is a fixed enum whose predicate members stop at
`STARTS_WITH`/`NOT_STARTS_WITH` — closed over what file min/max statistics can answer, because a
prefix is a range in disguise — and `ExpressionParser.toJson` is `static` with no extension point.
So no engine can serialize a new operation, however well it reads.

The workaround: **a predicate whose *term* carries the intent needs no new operation.** Every
serializer emits an ordinary equality unchanged.

```
SQL:  regexp_like(lower(msg), '(^|[^a-z0-9])TOKEN([^a-z0-9]|$)')
  ──▶ the engine's constraint carries the call
  ──▶ the Trino overlay recognises exactly the two shapes the analyzer writes
      (a plain token boundary, and the compound form for dotted or hyphenated
       identifiers) and emits:  equal("__kahshe_match__msg", "TOKEN")
  ──▶ ANDed into the Iceberg scan filter, serialised as an ordinary eq predicate
  ──▶ ContainsExtractor.sentinelHint strips it back into a MATCH hint
  ──▶ the term tier answers it
```

Only `eq` is accepted on the sentinel. Another operation means the producer and the reader
disagree about the encoding, and guessing would drop a predicate the engine believes it pushed.

This is a workaround for a gap in the **library**, not in the specification. The merged
expressions spec on `apache/iceberg` defines an `apply` form for exactly this, and permits a
vendor function catalog; `ContainsExtractor` already parses it. Nothing serializes it yet because
the released iceberg-java `ExpressionParser` has no `apply`.

`dev/trino-patch/` holds two overlaid Trino classes that do the recognition. They are a local
overlay rather than an upstream change, and they are type-checked and tested as part of
`./gradlew check` against the real Trino artifacts, with no Docker and no cluster.

---

## 8. What the watcher does

`watch/` evaluates detection rules against data as it arrives. It is a separate concern from
planning, it never touches the data plane, and it can run entirely on its own — no proxy, no
indexer, no index — reading whatever the rules name from tables where they already live.

Two evaluation paths, one output.

**The row scan** (default). `ScanPass` reads exactly the columns the loaded rules name out of
every data file the table has **added** since the last snapshot this process scanned, and
evaluates every rule **per row**. So a rule spanning several columns means what a detection
engineer reads it to mean — one row satisfying every field, not one file that happens to hold a
row for each — numeric fields work with no index at all, and a rule fires whether or not its
columns are indexed. The evidence is labelled `exact` on a delete-free snapshot and `advisory` on
one carrying delete files: the scan reads raw rows and applies no delete file, so on a merge-on-read
table `matched_rows` is an upper bound (`FileScanContext.confidence`, with the files counted on
`kahshe_watch_scan_delete_bearing_files_total`). Labelled rather than suppressed, because a
detection that goes silent on a merge-on-read table is worse than one that over-counts and says so.

`Scanner` is a **seam**, not a class: a scanner declares its name and the columns it needs for a
table, and gets a per-file hook plus the alerts it yields at the end. Scanners are discovered
through `java.util.ServiceLoader`, and one pass reads each added file **once**, projected to the
union of every scanner's columns. So a second detector — a content scanner, a rate detector, a
compiled ruleset — costs no second read. The rule-driven scanner is simply the first
implementation.

**The index-riding path** is the cheap case. Rules ride the indexing pass, so a file is evaluated
at the moment it is indexed with zero extra data reads. It sees one column at a time and only what
the index holds, so it takes `match`/`contains` rules on one indexed column whose file-level
answer is their row-level answer: a single field, or several under `any-of` — a file holding *a*
somewhere or *b* somewhere is a file with a row matching *a* or *b*. An `all-of` over two or more
fields does not commute that way (a row with *a* and a row with *b* is not a row with both) and
goes to the row scan, which has the row; so does every window rule, since an index that answers
"some file holds this token" would fire a rate rule on its first match. A negation, a nested
condition, a regular expression or a numeric range likewise goes to the row scan alone rather
than being half-evaluated per file. What does ride gets one verdict per field and the rule's own
condition tree, through the same `Conditions.eval` the row scan uses. `ReportPoller` picks up
alerts from build reports written by builds running elsewhere.

Both paths deliver through the same sink, in the same payload shape, under one shared (rule, file)
suppression — so a rule both paths can answer alerts once.

**Rules.** A rule is a list of field predicates over a table's columns, with `equals`,
`equals_ignore_case`, `match`, `contains`, `starts_with`, `ends_with`, `re` and the numeric
comparisons; values OR within an entry, entries AND within a selection. The condition is an
expression tree over named selections — `and`, `or`, `not`, parentheses, `1 of` and `all of` — the
shape a detection engineer already writes. Rules live in a YAML file, hot-reloaded on change.

Two shapes are **refused rather than warned about**, because in practice each is a typo: a
condition naming a selection that does not exist, and a selection the condition never uses.

**Confirmation SQL.** Every alert carries the SQL predicate that reproduces it against the file
the rows are in. The same condition tree has two interpreters — one over rows, one over the engine
— and they must agree; the null handling they share is written once so it cannot diverge in one of
them. A negated term compiles with an explicit null coalesce, because SQL's `NOT NULL` is NULL and
a bare negation would return fewer rows than the alert counted. A numeric comparison on a
non-numeric column compiles through a `TRY_CAST`, so a row whose text is not a number is excluded
rather than failing the whole query.

**Delivery is a seam too.** `AlertSink` implementations are found on the classpath through
`ServiceLoader`, so a sink kahshe does not ship joins by adding a jar — no fork, no edit to the
app. Three are built in, picked by name with `KAHSHE_WATCH_SINK`: `webhook` (the default), `log`,
and `none`, which evaluates and counts but delivers nothing — what an indexer-fleet member runs so
that one watcher elsewhere delivers, and alert dedup stays in a single process.

**Rules are prospective.** Files written before a rule existed are not re-examined, and the first
poll of a table starts at its current snapshot rather than walking history. A rule naming a column
the table's *schema* does not have cannot fire anywhere, so that is logged and gauged rather than
left silent.

**Sigma.** [`sigma/`](../sigma/README.md) is `pysigma-backend-kahshe`, which compiles Sigma rules
into kahshe rules rather than into a query — kahshe's rule form is already Sigma's shape, so the
conversion is structural. Nothing is ever dropped silently: any construct with no kahshe
equivalent raises by name, because a rule that converts with a clause missing loads, reviews as
correct, and fires on the wrong rows.

---

## 9. The module map

```
                    ┌─────────┐
                    │   app   │   main, roles, env → config records, external index IO
                    └────┬────┘
           ┌─────────────┼─────────────┐
           ▼             │             ▼
      ┌─────────┐        │        ┌─────────┐
      │  proxy  │        │        │  watch  │   (proxy and watch never depend
      └────┬────┘        │        └────┬────┘    on each other)
           └─────────────┼─────────────┘
                         ▼
                   ┌───────────┐
                   │  indexer  │   the read pass, orchestration, maintenance
                   └─────┬─────┘
                         ▼
                   ┌───────────┐
                   │  format   │   the artifact: readers, writers, coverage, pruner
                   └─────┬─────┘
              ┌──────────┴──────────┐
              ▼                     ▼
        ┌───────────┐         ┌───────────┐   (two leaves, and neither
        │ analysis  │         │  common   │    depends on the other)
        └───────────┘         └───────────┘
     what a value means:      metrics, caches,
     canonical forms, tokens  single-flight
```

The direction is one-way and enforced by the build files.

| Module | Package | Owns | Detail |
|---|---|---|---|
| `common` | `io.kahshe.common` | metrics registry, bounded and byte-weighed caches, single-flight, `Pem` | [common/README.md](../common/README.md) |
| `analysis` | `io.kahshe.analysis` | canonical value forms, the analyzer families | [analysis/README.md](../analysis/README.md) |
| `format` | `io.kahshe.format` | the three tiers' readers and writers, coverage, paths, scope, lease, build report, pruner, `DataFileIds` | [format/README.md](../format/README.md) |
| `indexer` | `io.kahshe.indexer` | `TableSource`, `IndexBuilder`, `BuildBudget`, `IndexSettings`, `IndexerService`, `IndexFreshness` | [indexer/README.md](../indexer/README.md) |
| `proxy` | `io.kahshe.proxy` | `KahsheHandler`, `Forwarder`, `AuthGate`, `AdminHandler`, `PlanRoutes`, `PlanService`, `CountRoutes`, `ContainsExtractor`, `BackendCatalogs`, `BackendTls`, `Mutations` | [proxy/README.md](../proxy/README.md) |
| `watch` | `io.kahshe.watch` | `WatchEngine`, `TableDiscovery`, rules and conditions, `Scanner`/`ScanPass`, `AlertSink` | [watch/README.md](../watch/README.md) |
| `app` | `io.kahshe` | `Kahshe` (main and roles), the `KAHSHE_*` environment read into the config records, `ServerTls`, `WatchRoles`, `IndexIo`, `CatalogSource`, `ClientDemo` | [app/README.md](../app/README.md) |

Notable constraints, each of them load-bearing rather than stylistic:

- **`analysis` depends on nothing of ours.** Not on `common`, not on Iceberg's artifact classes,
  not on the format — the JDK and a logging facade are the whole list. A watcher that only scans
  rows needs the analyzer and the canonical forms, and a second implementation in another language
  needs them as a specification — neither needs an index.
- **`format` never depends on the indexer, the proxy, the watch, or a storage client.** The
  artifact's readers and writers live together so they cannot drift; everything above them is
  someone else's problem. The one permitted backward edge is test-only: `format`'s round-trip
  tests build an index through `indexer` and borrow its fixtures.
- **`proxy` never depends on `watch`.** Detection does not ride the data plane.
- **Every `KAHSHE_*` variable is read in `app`**, and partitioned once into the per-module config
  records (`FormatConfig`, `BuildConfig`, `ProxyConfig`, `WatchConfig`). No module below reads its
  own configuration. There are two readers inside `app`, not one: `Kahshe.Config.fromEnv` takes
  everything else, and `ServerTls.fromEnv` takes the nine `KAHSHE_TLS_*` variables into
  `ServerTls.Settings` — a fifth record alongside those four, kept separate because only
  `app` binds the listening ports, so no module below it terminates TLS. The outbound direction is
  not the same: `proxy` is handed `KAHSHE_BACKEND_CA` inside `ProxyConfig`, because `Forwarder` and
  the catalog clients open their own TLS connections to the backend. Three reads happen outside that
  partition, and this is the whole list: `Fleet` reads `HOSTNAME` for the pod's own ordinal, which
  is identity rather than configuration; and two ceilings that exist only to be raised in an
  emergency are JVM system properties rather than config fields —
  `kahshe.watch.max.distinct.terms` (`TermCounts`) and
  `kahshe.bloom.build.max.bytes` (`IndexBuilder`). Adding a fourth is a design conversation: the
  list is only enforceable while it is short enough to name.

---

## 10. Deployment shapes

One distribution, selected by role.

- **`both`** (default) — proxy, indexer and, when rules are configured, the watcher, in one
  process. This is the small install, and it is the quickstart.
- **`proxy`** — data plane and admin, with the indexer separately switchable. In a split
  deployment the serving replicas run with the indexer off so serving latency is never behind a
  build.
- **`watch`** — no data plane at all. Discovery polls the catalog directly and the row scan reads
  what the rules name. With the indexer on it also builds the indexes for the tables its rules
  name; with the indexer off it builds nothing and instead delivers alerts from build reports
  written elsewhere, under the same suppression as its own scan.

Because the watcher needs only tables and never the REST passthrough, it can load any Iceberg
`Catalog` implementation by class name rather than requiring a REST endpoint. The proxy stays REST
by design — it *is* a REST catalog, and forwards everything it does not serve — so that setting is
ignored with a warning in the proxy roles rather than half-working.

Replicas share nothing. The plan cache is a per-process memo, not plan state: no plan store, no
paging, no cross-request continuation, so there is nothing to drain before a process goes away.
The blast radius is the other thing: a kahshe process that is down fails catalog calls, because
kahshe is in the metadata path — front it the way you front the catalog itself. Adoption is the one
table property plus the handful of decisions in the README's
[deploying section](../README.md#deploying) — TLS, the table cache TTL above one replica, and where
the index root lives.

---

## Further reading

- [FORMAT.md](FORMAT.md) — the index format, normatively
- [../CONTRIBUTING.md](../CONTRIBUTING.md) — toolchain, the gate, testing discipline, conventions
- [../README.md](../README.md) — the front page: what it does, the quickstart, compatibility, the measured results, deploying
- [CONFIGURATION.md](CONFIGURATION.md) — every `KAHSHE_*` variable and `kahshe.*` table property
- [ENDPOINTS.md](ENDPOINTS.md) — the served HTTP surface, the filter extensions, `_count`
- [OPERATIONS.md](OPERATIONS.md) — metrics, staleness alerting, index lifecycle, security posture
- [WATCH.md](WATCH.md) — the detection rule language
- [../helm/kahshe/README.md](../helm/kahshe/README.md) — the chart and the deployment shapes
- [../sigma/README.md](../sigma/README.md) — the pySigma backend
- [../dev/trino-patch/README.md](../dev/trino-patch/README.md) — the overlaid Trino classes
