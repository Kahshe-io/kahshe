# kahshe-indexer

Builds and maintains kahshe's sidecar indexes: which tables and columns get indexed, when, under
what memory budget, and holding which lease.

This module owns the write side. It decides that a column's index is behind the table's current
snapshot, takes an exclusive lease on that column, reads the data files no previous build covered,
and publishes new index artifacts beside the data. It does not read indexes back and it does not
prune anything — that is the `format` module's job, and the two meet only at the artifacts on
object storage. The module reaches a catalog exclusively through the `TableSource` interface, and
depends on `format`, `analysis` and `common`, never on the proxy or the watch subsystem.

The governing property is that **an index is advisory**. A file with no index entry is kept, never
skipped, so a table that has never been indexed, whose build failed, or whose index covers only
part of the snapshot still returns correct results — just with less pruning. Every failure decision
in this module falls out of that: refusing is always safe, so the code refuses early and loudly
rather than degrading quietly.

## Layout

```
io.kahshe.indexer
├── TableSource          the one seam to a catalog
├── BuildConfig          the knobs that size and bound a build
├── maintain/            WHEN to build
│   ├── IndexerService   passthrough-driven maintenance loop and its worker
│   ├── IndexFreshness   per-table observed-vs-built staleness gauges
│   └── Fleet            which replica this is, and its claim order
└── build/               HOW to build
    ├── IndexBuilder     one column build, end to end
    ├── IndexSettings    per-column / per-table / deployment setting resolution
    ├── BuildBudget      refuses a build whose knobs cannot fit the heap
    └── IndexBuildListener  the per-file observation hook
```

## The maintenance loop

`IndexerService` has no watcher, no scheduler and no durable queue. Catalog traffic drives it:

```
loadTable passthrough
        │  observe(prefix, namespace, table, columns, snapshotId)
        ▼
  IndexFreshness.observed()      always, even for observations the dedup drops
        │
  dedup on (table -> last handled snapshot)
        │
  bounded queue (256)  ──drop──▶ kahshe_indexer_jobs_dropped_total + WARN
        │
        ▼
  one daemon worker
        │  TableSource.load()  (re-loaded through invalidate() if the cached
        │                       view predates the observed snapshot)
        │  for each configured column, in Fleet.rotate() order:
        │      indexCurrent()?  ──yes──▶ skip
        │      IndexBuilder.buildColumn()
        │
        ▼
  IndexFreshness.built() / .failed()
```

When a `loadTable` response carries the `kahshe.index` table property — a comma-separated list of
column names — and the index is behind the table's snapshot, the table is queued and one daemon
worker rebuilds it with the service credential. Failures are logged, counted, and retried on the
next observation — per column, so one column's failure does not stop its siblings from building.
A column kahshe can never index (a type with no canonical form, or a leaf under a list or map) is
refused rather than retried: counted as `kahshe_index_columns_refused_total`, not as a build
failure, and claimed like a built one until the table's schema id changes, because no retry can
change a configuration error's answer. Because stale indexes only ever cost performance, the loop
needs no durability: losing the queue on restart costs a rebuild, not correctness.

`indexCurrent` is the check that decides a column is up to date, and it is stricter than "same
snapshot". An index is also out of date when its metadata is stamped `partial` (a build that died
or is still checkpointing), when it was cut under a different analyzer contract, or when it was cut
under a different gram rule or size. A contract change therefore starts a rebuild immediately
rather than waiting for the table to change; the reader keeps serving the old artifacts under their
own recorded contract while that runs.

### Freshness

The loop cannot announce that it has stopped. `IndexFreshness` exists so a scrape can: for every
`(prefix, namespace, table)` this process has observed declaring an index column, it keeps the
snapshot last seen, the snapshot last covered by a completed build, and when each happened. From
those it derives `kahshe_index_tables_tracked`, `kahshe_index_tables_behind`,
`kahshe_index_max_behind_seconds` and `kahshe_index_last_build_age_seconds`.

The state is per process and not durable — a restart resets it, and a replica that never sees a
table's traffic never tracks that table. It is bounded to 4096 entries, LRU, so a large catalog
cannot grow it without limit.

One rule is worth knowing before you read the code: **a process whose indexer is disabled reports
nothing behind.** It still tracks what it observes, so `tables_tracked` stays honest, but it cannot
close the gap and so the gap is not its to raise. A gauge that climbs without bound on a
correctly-configured, perfectly healthy proxy replica is the alert that gets muted, and then missed
on the pod where it meant something.

### Fleets

The unit of scale is the column, not the table and not the file. Set `KAHSHE_INDEX_FLEET` and N
replicas each poll the same tables and take a column's `BuildLease` when its index is behind,
skipping the columns another member already holds — counted as `kahshe_index_lease_skips_total`,
not as a build failure. A skipped table is left unclaimed so a later observation revisits it.

Nothing coordinates the members. With an identical claim order every replica would race the same
column first, lose to one winner, and skip its way down the same list: correct, and no faster than
a single replica. `Fleet.rotate` rotates each member's order by its own ordinal so N members start
on N different columns. The ordinal is configuration when set and otherwise derived from the
hostname (a trailing `-N` is read directly, anything else hashes to a stable value). It only has to
be stable for a pod's life — this is a spread, not an assignment, and two members colliding on one
ordinal costs a skip, never a wrong answer.

Outside a fleet, a lease held by someone else is a hard error: one builder per column means nothing
else is going to build it, so failing loudly is the honest answer.

## One column build

`IndexBuilder.buildColumn` is the whole write path. In order:

1. **Resolve the column by field id.** Properties are written by column *name*; artifacts are keyed
   by *field id*. `IndexSettings.resolveField` follows a name through the table's schema history,
   so a property written before a rename still applies and a renamed column keeps being maintained.
2. **Take the lease.** `BuildLease` is held for the whole build, so two builders of one column
   cannot interleave their metadata publishes. A snapshotless table or an unknown column takes no
   lease.
3. **Check the budget.** `BuildBudget.check` refuses, before the first data file is opened, a
   configuration whose committed memory cannot fit the heap.
4. **Read the prior generation.** The term metadata's file list is the source of truth for
   coverage. An index written by a *newer* format version is refused rather than rebuilt over.
5. **Plan the current files**, filtered by the table's declared `IndexScope` so Iceberg prunes
   partitions before a data file is opened, and sorted by location.
6. **Decide incremental or full**, reconcile coverage against the current file set, and select the
   files nothing covers.
7. **Read**, one wave of concurrent readers at a time, each appending into its own arena.
8. **Publish** every registered index type in cost order, then the build report.

Two paths short-circuit. If the prior index already covers every in-scope file and no renumbering
is pending, `restampOnly` republishes the metadata against the new snapshot and reads no data file.
It is not free: `decideIncremental` has already opened every prior aggregate leaf — up to 36 per
column — to prove them readable before the incremental decision is trusted, so an idle restamp
costs one Parquet open per range leaf. If `checkpoint-files` is configured, a build publishes after
that many new files and loops — ordinary publishes, each consumed by the next through the
incremental path, all under one lease, with every pass but the last stamped `partial`.

### Coverage, ordinals and tombstones

Index bitmaps name data files by **ordinal**. An ordinal is *allocated* once and recorded on its
coverage entry — it is not a file's position in any list. That single choice is what makes
incremental maintenance possible:

- A file that leaves the table is **tombstoned in place**. Every surviving bitmap still names
  exactly what it named before, so rolling retention is an ordinary incremental build rather than a
  full rebuild every day.
- A file that comes back is **revived, not re-read**. Iceberg data files are immutable, so the
  bitmaps naming its ordinal still describe it exactly.
- A dead ordinal is **never reused**, or a new file would inherit the bitmaps of the file that used
  to hold that number.

Sorting the file scan by location is a correctness requirement, not tidiness. Iceberg plans
manifests concurrently and returns files in an order that varies between runs; without sorting, two
full builds of one table would assign its files different ordinals, and a bitmap that names the
wrong files prunes away files that match.

Tombstones make a departure cheap but not free forever. Past 25% dead coverage *and* at least 16
dead entries, a build renumbers the survivors contiguously and drops the dead entries. Both guards
are needed: a fraction alone would renumber a three-file table on its first removal, which costs
more than the full rebuild tombstones replaced. When a build compacts, every gram and term bitmap is
translated through the same remap **in that same build** — coverage and bitmaps that disagree about
what a number means is exactly the false negative the format exists to prevent, which is also why a
compacting build does the full merge even when it has nothing new to read.

### When incremental is refused

Coverage records which *files* were read, not which *tiers* were written from them, and a build
must not patch what it cannot patch. A full rebuild is forced when:

- **A tier is enabled but absent.** An index built with the term tier off covers every file, so
  turning the tier back on would otherwise restamp and publish no term index at all, silently, and
  report success.
- **The analyzer contract changed.** Every publish stamps the current analyzer id. Patching an
  index cut under a different contract would relabel it while the old files' tokens under the new
  contract were never written — after which the reader probes for such a token, finds it absent,
  and prunes exactly the files that hold it.
- **The gram rule or size changed**, for the same reason.
- **The prior aggregate leaves are unreadable.** They are probed before any data file is read, so
  an unreadable leaf costs a full rebuild that self-heals rather than a failure after the whole
  table has been re-read.

### Memory is bounded by allocation, not estimation

Peak heap for the term tier is `readers × (arena + offset index)` — a count of fixed-size things,
allocated up front, before a file is opened. Each reader appends token occurrences into its own
`RunBuffer` arena and flushes its own sorted runs to a `TermRunStore`; nothing is coalesced per
file, nothing is shared, and there is no lock between readers. A file of prose and a file of two
billion distinct trace ids therefore cost the same.

That shape is deliberate. A growable structure bounded by an estimate of its per-entry cost cannot
be bounded at all, so there is no such structure. Where an estimate is genuinely unavoidable it is
rounded **up**, because the two errors are not symmetric: an over-count trips a valve early and
degrades a tier, which is slower and correct, while an under-count is an out-of-memory hours into a
build.

The remaining bounds follow the same asymmetry, and differ in what they do when crossed:

| Bound | Covers | On breach |
|---|---|---|
| `BuildBudget` | configured caches + arenas + gram ceiling vs. 80% of heap | refuse before opening a file |
| `KAHSHE_GRAM_BUILD_MAX_BYTES` (environment) | the gram map, checked every wave | drop the gram layer for this build; blooms still serve |
| the same cap, applied to a prior grams leaf | heap the leaf would cost to load | do not read it; gram coverage restarts at this ordinal |
| `kahshe.bloom.build.max.bytes` (JVM system property, `-D…`) | the per-file bloom map, which is O(files) | refuse the build |

The gram layer degrades and the blooms refuse, and that asymmetry is on purpose. Dropping the gram
layer mid-build costs pruning quality on a tier that is an optimisation over the blooms; dropping
the blooms would leave the metadata's leaf list and the coverage disagreeing about what was
written. Refusing is safe — the table keeps its previous index — and a refusal names both numbers
and what to change, which an out-of-memory does not.

### Reporting what cannot be seen elsewhere

Some conditions are only visible while a build holds the evidence, so the build says them:

- **Gram saturation.** A file whose distinct grams approach every gram its own alphabet can form
  (1000 for decimal trigrams, 4096 for hex) cannot be pruned by grams or blooms — every probe drawn
  from that alphabet finds all its grams present. It is served *correctly*, so the query-side
  symptom is indistinguishable from data that is simply not selective. Only the build has the gram
  sets in hand. Note that this is deliberately not the bloom's fill fraction: a bloom is sized from
  the gram count it is given, so fill measures sizing, not selectivity.
- **A no-op build.** A build with nothing to do and a build that indexed everything otherwise
  report themselves identically.
- **Coverage reconciliation.** A tombstone changes what the index covers without moving any
  counter.
- **Unreadable prior metadata**, which silently turns an incremental build into a full one.

Every build also writes a `BuildReport` (see `docs/FORMAT.md`) after the tier metadata, so a reader
that sees the report sees the build. Its counters are read back from the term document just
published rather than carried through the build, so the report says what the artifact says.

## Seams

Four extension points, in the order a third party is likely to need them.

### `TableSource`

The only seam between building and whatever catalog the deployment fronts.

```java
Table load(String prefix, TableIdentifier ident);
void invalidate(String prefix, TableIdentifier ident);
```

Implement it to run the indexer against a catalog kahshe does not front — a catalog embedding the
indexer implements it over its own `Catalog` object. `prefix` is the REST catalog's tenancy path
segment, empty where a catalog has none. `invalidate` matters: the worker calls it when an
observation carries a snapshot the cached table predates, and an implementation that ignores it
will wedge maintenance behind a stale view.

In-tree implementations are `io.kahshe.proxy.catalog.BackendCatalogs` and `io.kahshe.CatalogSource`.

### `IndexBuildListener`

Observes builds file by file, at the moment each file's term counts and exact gram set are
collected — before any leaf is written. Implement it to do something with a build's per-file
evidence that only exists during the read pass; this is how kahshe's watch rules are evaluated
during indexing, and how their verdicts ride out of the process in the build report.

Three things to know:

- **References handed to `BuildContext.file` are borrowed**, valid only for the duration of the
  call.
- **Answer `readsTermCounts()` honestly.** Tokenizing is the most expensive thing a build does, and
  the counts have exactly two consumers: the term dictionary and a rule that matches on tokens. A
  listener that reads only grams should say so. The default is `true`, which is the safe direction
  — a listener that forgets to answer gets the counts it may need and pays for them. Answer
  `false` and the `FileTerms` you are handed still carries the row and token totals, but nothing
  per token: `countOf` then reads 0 for every token, with no error and no counter.
- **Wrap with `IndexBuildListener.safe`.** It is applied once at the top of a build, so every hook
  below is safe to call bare and a listener failure logs a warning instead of failing a build. Note
  that `NONE` is a non-null no-op whose *identity* the build tests, which is why `listener != null`
  is never the right check.

### `IndexType`, through `IndexTypes`

The registry of index tiers lives in the `format` module and is discovered with `ServiceLoader`;
the built-ins are declared exactly the way a third party's would be. This module iterates the
registry rather than naming tiers: `IndexTypes.collectors(BuildContext)` gathers the collectors
that want to see rows, and `IndexTypes.inCostOrder()` drives the publish. A type that answers
`Collector.NONE` is dropped rather than called per row, so adding a type costs nothing to builds
that do not use it.

Implementing one buys a new pruning tier written from the same single read pass as the built-ins,
with no change to this module. A collector's hooks run on the thread reading that data file; one
file's calls are ordered and contiguous, but several files are read concurrently, so a collector
that keeps state must be safe for concurrent files.

### Table properties, through `IndexSettings`

The operator-facing seam. Every per-column knob resolves in one place and in one order —
`kahshe.index.<column>.<key>`, then `kahshe.index.<key>`, then the deployment default — so the
order cannot fork per knob.

| Property | Meaning |
|---|---|
| `kahshe.index` | the columns to index, comma-separated |
| `kahshe.index.scope` | an expression narrowing which data files are indexed |
| `analyzer` | `tokens` (ASCII tokens, lowercased) or `value` (the canonical value whole, exact and case-sensitive) |
| `max-token-length` | the cap on an indexed token |
| `ngram` | gram size |
| `term-index`, `gram-index` | tier participation |
| `bloom-fpp` | bloom false-positive rate |
| `checkpoint-files` | publish every N new files instead of once at the end |

Three rules hold across all of them:

1. **A malformed value warns and falls through to the next level**, rather than silently becoming
   `false` or `0`. Only the literals `true` and `false` are accepted for a flag — deliberately not
   `Boolean.parseBoolean`, which reads every typo as `false`, and for a tier toggle `false` is a
   tier silently switched off.
2. **The deployment flag is a ceiling, not a default.** A property may narrow a tier for one column
   but never widen one the deployment has off: the build budget and the serving caches are both
   sized from the deployment flags, so a tier built past them would be unbudgeted at build time and
   never opened at serve time. A widening attempt warns, because an operator who set it must see
   that it did nothing.
3. **Every artifact records what it was built under** — the analyzer id carries the token cap, a
   serialized bloom carries its own geometry, the metadata records the gram rule and fpp — so a
   reader never interprets an artifact from its own configuration. Preserve that property when
   adding a knob here.

## Deployment knobs

| Variable | Effect |
|---|---|
| `KAHSHE_INDEXER` | whether this process drains the build queue at all |
| `KAHSHE_INDEX_FLEET`, `KAHSHE_INDEX_FLEET_ORDINAL` | fleet membership and claim order |
| `KAHSHE_INDEX_THREADS` | concurrent data-file readers, and so arenas |
| `KAHSHE_TERM_BUFFER_BYTES` | size of one reader's arena |
| `KAHSHE_TERM_BUILD_DIR`, `KAHSHE_TERM_BUILD_MAX_SPILL_BYTES` | local scratch for sorted runs |
| `KAHSHE_GRAM_BUILD_MAX_BYTES` | the gram valve |
| `KAHSHE_NGRAM` | default gram size |
| `KAHSHE_INDEX_STALE_WARN_MS` | how long a table may sit behind before a warning |
| `KAHSHE_MAX_TOKEN_LENGTH` | the analyzer's token-length cap, default 256. It is part of the analyzer id, so changing it changes the contract an index was built under and forces a full rebuild rather than an incremental one |

Every row above is an environment variable. The one bound that is not is the per-file bloom map's
cap, `kahshe.bloom.build.max.bytes` (default 1 GiB), which is read as a JVM system property —
raising it means `-Dkahshe.bloom.build.max.bytes=…` on the command line, not an exported variable.

## What this module deliberately does not do

- **It does not read or serve indexes.** No pruning, no plan, no cache. Those live in `format` and
  `proxy`; this module and the read side meet only at the artifacts.
- **It does not talk to a catalog.** Every table comes through `TableSource`. There is no REST
  client, no credential handling and no catalog-specific code here.
- **It does not schedule.** There is no cron, no watcher and no durable queue — maintenance is a
  consequence of traffic, and a lost queue costs a rebuild rather than correctness.
- **It does not coordinate a fleet beyond the lease.** No leader, no assignment, no membership
  protocol. Rotation is a spread; the lease is the only thing that is authoritative, and a
  collision costs a skip.
- **It does not shard one column's build.** The column is the unit of parallelism across
  processes; within a build, files are.
- **It does not index a leaf *inside* a list or map.** A `list<T>` or `map<K,V>` whose element or
  value type has a canonical form is indexed by naming the container (`tags`), as the union of its
  members' terms — a map contributes its values, and under a whole-value contract a row contributes
  each distinct term once (see `docs/FORMAT.md` §6.7). A path *through* one (`tags.element`,
  `props.value`) stays refused, because a bitmap over file ordinals cannot answer a predicate keyed
  there without losing rows; the refusal names the repeated field it passed through.
- **It does not make an index required.** Nothing here can make a query wrong by being absent,
  incomplete or stale — which is what licenses every "refuse" in the list of bounds above.

## Tests

```sh
./gradlew :indexer:test
```

The suite covers the build path against real local Parquet tables written by
`LocalTableFixture` (a test fixture this module lends to every other module, alongside
`RecordingListener`), plus the lease, budget, fleet and freshness behaviour. Tests run at a scale
where no memory budget is ever exercised, which is precisely why `BuildBudget` checks arithmetic
instead: an impossible configuration passes every test and then dies hours into a real build.
