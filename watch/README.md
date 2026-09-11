# kahshe watch

Prospective detection over Iceberg tables: rules are evaluated against data files as they land,
and each match becomes an alert carrying the SQL an operator can run to confirm it.

`watch` turns a rule file into alerts. It never queries your engine and never rewrites your data.
It learns about new files two ways — by riding the index builds kahshe's indexer already performs,
and by reading the columns a rule names out of each newly added data file — evaluates the rules
against what it sees, and hands the resulting JSON payload to a sink. Rules are **prospective
only**: a table is picked up at its current snapshot and its history is never walked, so a watcher
never reads years of data to start. Detection is the rules' job rather than the index's, so a rule
fires on a column no index covers; the index-riding path is an optimisation for the subset of
rules a file's term counts can decide, not a precondition.

---

## Architecture

```
 KAHSHE_WATCH_RULES (YAML)
          │
      WatchRules ──────────────┬──────────────────────────┐
   (hot reload on mtime)       │                          │
                               ▼                          ▼
                        TableDiscovery              WatchEngine
                     (polls rule-named           (IndexBuildListener:
                      tables, feeds the           rides index builds,
                      indexer's observe)          per-file term counts
                               │                   and gram set)
                               ▼                          │
                          ScanPass  ────────┐             │
                    (reads new data files   │             │
                     once, projected)       │             │
                               │            │             │
                      ┌────────┴────────┐   │             │
                 RuleScanner      WindowScanner           │
                (row verdicts)   (N per key in T)         │
                      └────────┬────────┘                 │
                               │                          │
                               ▼                          ▼
                            Alerts  ◀──────────────  ReportPoller
                   (payload + (rule, file) claim)   (delivers alerts
                               │                     other processes
                               ▼                      recorded in a
                          AlertSink                   build report)
                   webhook · log · none
```

### The types, and what moves between them

| Type | Role |
|---|---|
| `WatchRules` | Loads and validates the YAML rule file; re-checks its mtime on every `current()` call, so rules hot-reload. Validation is per rule and fail-open — an invalid rule is skipped, loudly and counted, and the rest of the file loads. |
| `WatchRule` | One validated rule: a table, a list of `WatchRule.Field` predicates over its columns, a `WatchRule.Expr` condition tree over those fields, and an optional `WatchRule.Window`. |
| `ConditionParser` | Parses the `detection` form's Sigma-style condition (`and`, `or`, `not`, `1 of`, `all of`, `them`, `sel_*` patterns) into an `Expr` tree. |
| `TableDiscovery` | A daemon poll over the distinct `(prefix, table)` pairs the loaded rules name — never a namespace listing. Feeds the indexer's observe path for columns `kahshe.index` declares, and hands **every** polled table to the row scan. |
| `WatchEngine` | An `IndexBuildListener`. As an index build reads a file it sees that file's term counts and its gram set, cut by the column's own `Grams.Contract` rather than a fixed size, and evaluates the rules those can decide. |
| `ScanPass` | The one read of new data files. Projects the union of the columns the registered scanners ask for, reads each added file once, and hands every row to every open `FileScan`. |
| `RuleScanner` | The first `Scanner`: evaluates rules per row, answering every operator including the ones no index can decide (`re`, `gt`/`gte`/`lt`/`lte`, `equals`, `equals_ignore_case`, `starts_with`, `ends_with`). |
| `WindowScanner` / `WindowCounters` | Rate detection: N matching rows for one key within a timeframe, counted across files. |
| `PreparedRule` | One rule resolved against a table's column kinds — literals canonicalised, regexes compiled, comparison values parsed — and the per-row test that follows. Shared by both scanners so they agree on what "this row matches" means. |
| `Conditions` | Evaluates an `Expr` tree against one row's field hits. |
| `ConfirmationSql` | Compiles the same tree into the Trino-dialect, snapshot-scoped SQL carried in the alert. |
| `Alerts` | The one payload shape and the one `(rule id, file path)` suppression claim, shared by every evaluator. |
| `AlertSink` | The one way an alert leaves the process: `WebhookSink` (default), `LogSink`, `NoneSink`. |
| `ReportPoller` | For a process running with the indexer off: reads each watched column's build report and delivers the alerts a build elsewhere raised, under the same claim. |

A process runs some subset of three roles: the index-riding listener (needs the indexer on),
discovery plus the row scan (wherever rules are configured), and the report poller (when the
indexer is off, so that alerts raised by other processes still get delivered).

---

## The seams

Two `ServiceLoader` SPIs. Both are how kahshe's own built-ins are registered, so a third party's
implementation is not a second-class citizen.

### `Scanner` — a row-level evaluator over new data files

Declared in `META-INF/services/io.kahshe.watch.scan.Scanner`, with a public no-argument
constructor. `Scanners.discover` finds them; `Scanners.BUILT_INS` fixes the order the built-ins
run in and anything else follows.

What implementing one buys you: **the read**. `ScanPass` asks every scanner which columns it needs
(`columns(TableView)`), projects the union, reads each newly added file once, and delivers every
row to every open `FileScan`. A second scanner therefore costs no second read of the file, and a
scanner that answers "no columns" for a table costs nothing at all. You also inherit the shared
alert payload and suppression, the parallel reader, and the replay machinery.

| Method | Contract |
|---|---|
| `name()` | This scanner's name, stable and unique. It appears in the pass's logs and is what `Scanners` orders the built-ins by. |
| `columns(TableView)` | The columns of this table you need read, resolved against its schema. Never name a column the schema lacks — the projection would fail and take every other scanner's evaluation of that file with it. |
| `open(FileScanContext)` | A per-file evaluation, or `null` for nothing to do. Called concurrently; the `FileScan` it returns is used by one thread only. |
| `orderRowsBy(TableView)` | Opt in to sequential, time-ordered file reads (see below). Default `null`. |
| `replayMs(TableView)` | How far back you need the table replayed on first sight. Default `0` — prospective only. |
| `configure(ScanContext)` | The one lifecycle call, before any table is scanned. |

`FileScanContext`, `TableView` and `Row` are the scanner's whole view of the world, and they name
no table format. `ValueKind` — analysis's own vocabulary for what a column's values *are* — stands
where an Iceberg `Type` would.

### `AlertSinkProvider` — where alerts go

Declared in `META-INF/services/io.kahshe.watch.sink.AlertSinkProvider`. `AlertSinks.fromEnv`
selects exactly one by `KAHSHE_WATCH_SINK`. Shipping a provider on the classpath is the whole
integration; nothing in this module or the app is edited for it.

Two constraints on an implementation: `deliver` must not throw and must not block its caller on
the network (the caller is an index build or a scan), and `awaitDrain` must block until what has
been handed over is delivered or given up on, since CLI builds call it before JVM exit. Every
provider on the classpath is constructed just to be asked its `name()`, so a constructor must do
nothing but exist.

---

## Why the code has this shape

### Two evaluators, one claim

The index-riding engine and the row scan overlap: a single-column `match`/`contains` rule on an
indexed column can be answered by both. `Alerts` therefore owns a `(rule id, file path)` claim
that both consult, so such a rule alerts once per file rather than twice.

`WatchRule.ridesIndex()` is what keeps the split honest. It admits a rule only when it names one
column, uses only operators a file's term counts or gram set can decide, carries no window, and
has a shape whose file-level answer is its row-level answer: one field, or `any-of` over several.
An `all-of` over two or more fields is not half-evaluated on the cheap path — a file-level
conjunction could only establish that the file holds a row matching each conjunct, never that one
row matches them all, which is not what the rule says; OR has no such gap, because a file with a
row matching *a* or a row matching *b* is a file with a row matching *a or b*. A window rule never
rides: evaluated per file on first match, "N within T" turns into "ever". The index path decides
per FIELD and hands the rule's own tree to the same `Conditions.eval` the scan uses, so a rule
means one thing on both roads.

Delivery is at-least-once by construction. The claim is a bounded in-memory cache, so eviction, a
restart, or a compaction that rewrites file paths can produce a repeat. A duplicate alert is a
cost; a missed one is a defect, and the module resolves every such trade in that direction.

### The condition tree has exactly two interpreters, and they must agree

`WatchRule.Expr` is data: leaves indexing a field list, with `and`/`or`/`not` above them. Two
things read it — `Conditions.eval`, which decides whether one row satisfies the rule, and
`ConfirmationSql`, which compiles it into the predicate an operator runs on their own engine.

The place they nearly disagree is null. A null column does not satisfy a field on the row side —
`false`, not *unknown* — so `not` of that field holds and the row fires. SQL's three-valued logic
says otherwise: a predicate over `NULL` is `NULL`, and `NOT NULL` is `NULL`, which drops the row.
So the SQL side wraps every negated term in `COALESCE(…, false)`. Without it the confirmation
returns fewer rows than the alert counted, which reads as over-alerting when nothing over-alerted.
A third interpreter would owe the same agreement, which is why the two live side by side in one
package.

Relatedly, `ConfirmationSql` emits **every** field's predicate, not only the ones that matched: the
SQL has to ask the rule as written, and a condition missing half its conjuncts would confirm
something else.

### The confirmation SQL is written, never executed

kahshe never runs SQL. The operator's own engine runs the alert's `confirmation_sql`, under the
operator's own authorization, and that is the point — the alert is advisory and independently
checkable. It is pinned to one snapshot and one file path, so re-running it later answers about
the same rows.

Two shaping decisions follow from that:

* Token and `contains` evidence both compile to `position(...) > 0` — a superset pre-filter, since
  `position()` matches beyond token boundaries. The query can over-return, never under-return.
  Only the `re` operator emits `regexp_like`, because only there did the rule ask for a regular
  expression.
* A window rule's confirmation is a grouped count over the whole table, bounded by the window's
  time range and written for the **kind** of the time column: an instant literal marked UTC for
  `timestamptz`, a wall-clock literal for `timestamp`, a date literal for `date`, raw epoch millis
  for an integral column, ISO-8601 text for a string one. Two constraints force this. Each form
  must compare without consulting the reader's session time zone, or the same confirmation returns
  different rows for different operators — and a confirmation that returns nothing reads as a false
  positive rather than as a broken query. And each must leave the column *bare*: wrapping it in
  `AT TIME ZONE` answers correctly but hides the column from Iceberg's min/max pruning, which is
  what keeps a window confirmation to the window's own files instead of the whole table.

### Windows are the one stateful shape, and their costs are named

Every other evaluator here is stateless per file, which is what makes them restartable and
order-free. A window cannot be answered that way: five failed logons in five minutes routinely
arrive in three different files, so a per-file count undercounts the window and the rule silently
does not fire. `WindowScanner` holds the state that answers it, and holds the costs in one place
where they can be read.

`WindowCounters` keeps, per key, the `N` most recent event times in a `long[]` of exactly `N`
slots. That answers the threshold *exactly*: a window of N events inside T ends at some event, and
at the moment that event is offered the array holds those N or a tighter set, so
`ts[N-1] - ts[0] <= T` decides it. Nothing older can matter — an older event could only widen a
span already too wide. So memory per key is N longs and does not grow with traffic. What it grows
with is **keys**: a rule keyed by client IP over a busy web log has millions. Hence a cap
(`KAHSHE_WATCH_WINDOW_MAX_KEYS`, default 200,000), LRU within each of 16 hash shards, and a counter
for evictions — an eviction throws away a partial window, which is a miss. Log time is not arrival
time, so an event older than everything retained cannot be placed and is dropped and counted too.
After a trip the key's slots are cleared, so the rule fires once per N events rather than once per
event thereafter.

Sharding is by key hash with a lock per shard rather than one lock per rule: the row path is a few
tens of nanoseconds, and a single lock would serialise every scan thread through it.

`min_count` and `window` are deliberately separate keys, and a rule carrying both is refused rather
than merged. `min_count` counts rows within one data file — an artefact of how the table was
written — while a window is a statement about time. One word with two meanings is how a rule comes
to have two thresholds and no way to say which wins.

### Ordering costs parallelism, and only where a rule asks for it

`ScanPass` reads files concurrently. That interleaves event time between threads, and a scanner
holding state *across* rows can be wrong when rows arrive out of order — a window rule run that way
drops events as unplaceable and raises fewer alerts than the same rule on one thread, silently but
for a counter.

`Scanner.orderRowsBy` is the opt-in, and it buys two things together because neither alone is
enough: the pass reads that table's files **one at a time**, and it reads them in ascending order
of their lower bound on that column, which Iceberg already records per file. Ordering without
sequencing still interleaves; sequencing without ordering trusts whatever order the manifest
happened to list. A file whose bound is missing or unreadable sorts last rather than first — it is
the file whose position cannot be argued for, and putting it after the ones that can keeps the
ordered prefix genuinely ordered.

The cost is the parallelism for every scanner on that table, since the pass reads each file once
for all of them. It is paid only where a rule asks for it.

### Replay: the data files are the durable state

A stateful scanner cannot start empty — a window rule that restarts mid-window silently never fires
it. So `Scanner.replayMs` asks for the span its state covers, and `ScanPass` reads the files added
within that span **for that scanner alone**. The prospective scanners never see them; replaying
older files through the row scanner would alert on every row of the last window.

Replaying data files rather than persisting counters means there is no second format to write,
version or garbage-collect. What replay cannot rebuild is which windows already alerted, so a
window that fired shortly before a restart fires again, marked `replayed` in the payload — a
duplicate rather than a miss. The payload carries `window_key` and `window_end_ms` so a receiver
can dedupe on them. Replay is bounded (`KAHSHE_WATCH_REPLAY_MAX_FILES`, default 2,000): a day-long
timeframe on a busy table is otherwise an unbounded startup read, and a watcher that takes an hour
to become ready is not watching. Truncating leaves state unrebuilt, so it warns with the three ways
out — raise the budget, shorten the timeframe, or accept the gap — and counts.

### The port: nothing above the reader names a table format

`ScanPass` is the Iceberg reader, and it, its row adapter `ProjectedRow`, and `HuntPass` — the
other reader, which enumerates every file the table holds to answer one term over all of them from
the term index — are the only files in the evaluation path that name a table format. Above them,
scanners see `TableView` (a name and each
top-level column's `ValueKind`), `FileScanContext` (the prefix, table, snapshot, path, replay flag,
delete-bearing flag, column kinds, and the set of columns that hold many values per row) and `Row`
(the value plus its canonical, lowercased and tokenized forms, derived lazily and memoized per
row). `PreparedRule` resolves against kinds, not types.

That boundary is checked by the compiler rather than by intent: a scanner reaching for a table
format's own types will not build. The practical consequence is that serving a second table format
is one reader and one `TableView`, not the rules, the conditions, the windows or the alerts.

Only top-level columns appear in a `TableView`. A rule naming a nested path finds nothing and is
counted as uncovered by discovery, rather than being projected and then failing on every row.

`ProjectedRow` is the Iceberg row adapter and is reused across the rows of one file, because
otherwise a wide file allocates a map, a lowercase copy and a token list per row per scanner. It
reads **by position**, not through `Schema.accessorForField`: those accessors are built for
Iceberg's internal representation, where a `timestamp` is a `Long` of microseconds, while the pass
reads through the generic data model, where the same column is a `LocalDateTime`. The record's
field order is the projection's, so the position is the field.

A **list or map** column is read member by member: `Row.arity` says how many values the row holds
for a column, and `canonical`/`lowered`/`tokens` take a member index, so an operator never sees
Java's rendering of a collection. It used to — the adapter fell through to `String.valueOf`, which
made `contains 'alpha, bravo'` match a row whose tags are `["alpha","bravo"]` while
`equals 'alpha'` did not. Members are **copied** out of the reader's collection on read, because
Iceberg hands back one instance per column and clears it for the next row. The member-aware methods
are defaults on `Row` that answer the scalar reading, so an adapter written before containers —
another format's, a test double's — is unaffected.

`PreparedRule` applies the operator per member and is satisfied by ANY of them, matching what the
index proves. Because the quantifier is per field, two conditions on one container column are
refused at prepare time (`PreparedRule.UnsupportedRule`, logged and skipped, not fatal) rather than
answered with a quantifier the rule does not read as — see [WATCH.md](../docs/WATCH.md) §3.

### Loading refuses ambiguity, and fails open around it

An invalid rule is skipped with a warning and counted; the rest of the file still loads, and a
file-level parse error keeps the previous rule set. Within that, the loader refuses anything that
could be read two ways rather than picking one:

* The three rule forms — single `column`, multi-column `where`, and Sigma-shaped `detection` — are
  exclusive. A rule carrying two of them has two answers to "which columns does this name", and the
  quiet one would win.
* A `detection` rule may not also carry a top-level `condition`; its condition lives inside it.
* Duplicate rule ids are refused after the first, because ids key the alert claim and a collision
  would silently share state.
* `no`, `off`, `yes` and `true` are **booleans** in YAML, so a rule matching the literal string
  `no` would match `false` instead — it loads, reviews as correct, and never fires. That is refused
  with the fix (quote it) in the message.
* A `match` entry must normalise to exactly one indexable analyzer token, validated against the
  default tokens contract, since the loader has no index in hand to ask.
* A window needs an explicit `ts_column`: which column carries event time is not guessable. Its
  `count` must be at least 2, since a threshold of one is a rule without a window.

`equals_ignore_case` exists because the rest of the text vocabulary is already case-insensitive and
because Sigma's plain `field: value` is case-insensitive in most implementations. It folds the
*canonical* form on both sides, so `"07045"` in a rule still meets `7045` in an int column exactly
as `equals` does.

### Failure containment

A poisoned rule must not take down the others, and one unreadable file must not lose the alerts of
the rest. Both evaluators catch per rule; `ScanPass` catches per file and per scanner; the webhook's
drain thread catches per payload. Discovery's loop catches `Throwable`, because an `Error` there
would silently end the sole discovery thread. Everything caught is logged and counted.

---

## Configuration

| Variable | Meaning |
|---|---|
| `KAHSHE_WATCH_RULES` | Path to the YAML rule file. Unset means no watch roles run at all. |
| `KAHSHE_WATCH_SINK` | `webhook` (default), `log`, `none`, or a discovered provider's name. An unrecognised name fails startup rather than falling back, and the message lists what was discovered. |
| `KAHSHE_WATCH_WEBHOOK` / `_AUTH` / `_TIMEOUT_MS` | Endpoint, `Authorization` header value, per-request timeout. Validated at startup: a malformed URL or header disables the webhook with an ERROR rather than throwing later on the drain thread. |
| `KAHSHE_WATCH_POLL_MS` | Discovery and report-poll interval. |
| `KAHSHE_WATCH_SCAN` | `false` builds no row scan at all, leaving only the index-riding path. |
| `KAHSHE_WATCH_SCAN_THREADS` | How many data files the scan reads at once. |
| `KAHSHE_WATCH_WINDOW_MAX_KEYS` | Live keys a window rule may track before the oldest are evicted. |
| `KAHSHE_WATCH_REPLAY_MAX_FILES` | Files one table's first-sight replay may read. |
| `KAHSHE_WATCH_REALERT_ON_REBUILD` | Whether a full rebuild of already-covered files re-alerts. |
| `KAHSHE_WATCH_SQL_CATALOG` | The catalog name the confirmation SQL is written against. |

Every counter that stands for a *miss* is exported, because a silent false negative is the failure
mode this module is built to avoid: `kahshe_watch_window_key_evictions_total`,
`kahshe_watch_window_late_drops_total`, `kahshe_watch_replay_truncated_total`,
`kahshe_watch_counts_truncated_total`, `kahshe_watch_reports_missed_total`,
`kahshe_watch_rules_skipped_total`, `kahshe_watch_rules_uncovered`,
`kahshe_watch_webhook_dropped_total`.

---

## What this module deliberately does not do

* **It does not query your engine.** No SQL is executed anywhere in this module. Confirmation SQL
  is text in a payload, for a human or a downstream system to run under its own authorization.
* **It is not retrospective.** A table is picked up at its current snapshot. There is no hunting
  mode, no backfill over history, and a rule added today says nothing about last month. The only
  backward read is the bounded first-sight replay a stateful scanner asks for.
* **It does not keep durable detection state.** Window counters live in memory and are rebuilt by
  replaying data files; the alert claim is a bounded in-process cache. Two watchers do not share
  state, and a restart re-reads rather than resumes.
* **It does not deduplicate across processes.** The `(rule, file)` claim is per process. The
  intended shape for a fleet is that members evaluate with the `none` sink and record alerts in
  their build reports, while one watcher elsewhere delivers from those reports — which puts dedup
  back in a single process.
* **It does not evaluate nested columns.** Only top-level columns are visible to a rule; a rule
  naming a nested path is reported as uncovered rather than silently matching nothing.
* **It does not list namespaces or discover tables on its own.** Only the `(prefix, table)` pairs
  the loaded rules name are polled — no wildcards in a rule's `table`.
* **It does not guarantee exactly-once delivery.** See the at-least-once note above; the payload
  carries enough (`rule.id`, `file.path`, and for windows `window_key` and `window_end_ms`) for a
  receiver to dedupe on its own terms.

---

## Tests

```sh
./gradlew :watch:check
```

`check` includes `javadoc`, so a `{@link}` left pointing at a member a rename moved fails the build
rather than the review. The suites pin what this README claims: that the condition tree's two
interpreters agree on a null column, where a bare `NOT` in SQL would otherwise drop a row the row
side counted; that a conjunction across columns needs one row satisfying all of it rather than one
file holding a row per conjunct; that a window counts across files, fires once per N events,
survives a restart by replay, and counts every way it can miss; that a window bound is a literal
the reader's session zone cannot move, for each kind of time column; that the loader refuses an
ambiguous rule rather than picking a reading, and still loads every rule the Sigma backend
generates; and that Iceberg is named by five classes and no others — the reader, its row adapter,
the discovery and delivery pollers, and the hunt, which enumerates a table's files for itself —
checked deny-by-default over every compiled class in the module.

## Merge-on-read

`ScanPass` reads raw data files and applies no delete file, so on a snapshot carrying deletes its
counts are upper bounds and `FileScanContext.confidence()` reports `advisory` instead of `exact`.
The proxy refuses such a snapshot outright, because there a wrong file list is a wrong answer; a
detection cannot refuse without going blind, so it alerts and labels the evidence.

The index-riding path makes the same distinction, for the same reason: a build reads raw data
files and applies no delete file either, so on a delete-bearing snapshot its term counts are
upper bounds. `WatchEngine` takes the verdict through a `Deletes` seam rather than reaching a
snapshot itself — `watch` names no table format — and `SnapshotDeletes` in `app` answers it from
the catalog. It fails CLOSED: a table that will not load, a snapshot that is gone, a summary with
no count all read as delete-bearing, because the claim being made is exactness and an unproven
claim of exactness is the error a watcher may not make. An engine wired without the seam labels
everything `advisory`. `kahshe_watch_build_delete_bearing_total` counts the builds where it bit,
and the WARN fires only where a token rule is loaded — `contains` evidence is advisory anyway and
has no count to bound.
