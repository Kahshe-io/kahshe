# kahshe watch — detection rules

kahshe watch evaluates detection rules against data as it arrives, where the data already is. No
ingest pipeline, nothing copied, nothing entering the query path. It never executes SQL and never
returns row content: every alert carries the SQL an operator runs in their own engine, under their
own authorization, to see the rows.

A `KAHSHE_MODE=watch` instance with `KAHSHE_INDEXER=false` needs no proxy, no indexer and no
index — point it at a catalog and a rules file. `KAHSHE_CATALOG_IMPL` loads any Iceberg `Catalog`
by class name, so that catalog need not be REST: this works on Glue, Hive and JDBC as well as on
Polaris, Nessie, Unity and Lakekeeper.

---

## 1. The two evaluation paths

**The row scan** (`KAHSHE_WATCH_SCAN`, on by default) reads exactly the columns the loaded rules
name out of every data file the table has **added** since the last snapshot this process scanned,
and evaluates every rule **per row**. That is what makes a rule spanning columns mean what a
detection engineer reads it to mean — one row satisfying every field, not one file holding a row
for each — and it is what lets a rule fire on a column `kahshe.index` does not name.

**The index-riding path** is the cheap case: rules ride the indexing pass, so a file is evaluated
at the moment it is indexed with zero extra data reads. It sees one column at a time and only what
the index holds, so it takes only single-column `match`/`contains` rules on indexed columns, and
only shapes whose **file**-level answer is their **row**-level answer: one field, or `any-of` over
several. An `all-of` over two or more fields is the row scan's — a file with each token *somewhere*
is not a row with both, and the two paths used to answer that rule differently. A window rule never
rides the index, for the same kind of reason: evaluated per file on first match, "N within T" turns
into "ever". A `not`, a nested condition, a regex or a numeric range goes to the row scan alone
rather than being half-evaluated per file.

Both deliver through the same sink, in the same payload shape, under one shared (rule, file)
suppression, so a rule both paths can answer alerts once.

**Rules are prospective.** Files written before a rule existed are not re-examined, and the first
poll of a table starts at its current snapshot rather than walking its history.

**A term is not.** `kahshe hunt <prefix> <ns.table> <column> <term>` evaluates one token over
every data file the table holds, from the term index alone and with no data read — the question a
new indicator raises, "were we already hit?". Each live file is a `hit` (the dictionary holds it),
a `miss` (the dictionary proves it absent) or `unresolved` (outside the index's coverage: not
examined, and reported as such rather than folded into either answer). It refuses, naming the
reason and exiting 3, wherever the plan path would keep every file: an unknown column, a column
with no term index, a value that is not one token the index's analyzer admits, an unreadable leaf.
It never alerts, so it never claims a (rule, file) the live watcher has yet to reach.

`kahshe hunt --rule <id>` asks the same of a rule from `KAHSHE_WATCH_RULES`: a `match` rule on one
column, one field or `any-of` over several — the shape `WatchRule.ridesIndex` admits, and the
refusal for any other shape is the reason that method gives (`whyNotRidesIndex`), so the hunt and
the watcher cannot disagree about what the index can answer. Each file's verdict goes through the
same condition interpreter the watcher uses. Two shapes that ride a build are refused on top:
`contains` (the gram tier's verdict is advisory, and on identifier-dense text every file holds
every gram) and `min_count` above one (the dictionary records which files hold a term, not how
many times each). A trailing `--out <file.jsonl>` writes the result set: one `summary` record —
what was asked, of which table and column, the snapshot, the analyzer, the counts — then one
`file` record per hit and per unresolved file with its verdict. A miss needs no action, so it is
counted and not listed. It is a file an analyst keeps, not an alert: nothing goes through a sink.

The summary carries a `confirmation_sql`, in the same dialect and from the same condition compiler
as an alert's, snapshot-scoped and pinned with `"$path" IN (...)` to the hits **and the unresolved
files** — the only files a matching row can be in. Over the unresolved files the engine applies
the real predicate row by row, so the query is also the scan the hunt declined: the gap the index
left closes at the engine's cost, under the operator's own authorization. A miss is never pinned.
Past 500 candidate files the pin is dropped rather than truncated, and the query scopes by snapshot
alone. No hit and no unresolved file means no query at all. The `exact`/`advisory` confidence label
on a delete-bearing snapshot and an in-process scan of `unresolved` are not built yet.

**Dead rules are never silent.** A rule naming a column the table's **schema** does not have
cannot fire anywhere: kahshe logs it (rate-limited, naming the column) and gauges it
(`kahshe_watch_rules_uncovered`). Index coverage is not that condition — an unindexed column is
fine, the row scan reads it.

---

## 2. The rule file

`KAHSHE_WATCH_RULES` points at a YAML file, hot-reloaded on mtime change. A worked example ships
at [`helm/kahshe/examples/watch-rules.yaml`](../helm/kahshe/examples/watch-rules.yaml).

```yaml
rules:
  # Single-column form. Answered by the index-riding path when the column is
  # indexed and the condition is any-of (each token is its own field, so an
  # all-of over two is the row scan's), and by the row scan either way.
  - id: error-burst
    title: Error burst in event logs
    severity: high            # info|low|medium|high|critical; required
    prefix: lakehouse         # catalog prefix, as engines address it; required
    table: logs.events        # ns.table, no wildcards
    column: msg
    match: [error, exception] # single analyzer tokens — exact per-file counts
    condition: any-of         # any-of (default) | all-of
    min_count: 50             # token occurrences in one file (index path);
                              # matching rows in one file (row scan)

  # Multi-field form: a conjunction across columns, evaluated per ROW.
  - id: moriya
    title: ZzNetSvc service installation
    severity: critical
    prefix: lakehouse
    table: logs.events
    where:
      - { column: provider_name, equals: ["Service Control Manager"] }
      - { column: event_id,      equals: [7045] }
      - { column: service_name,  contains: [ZzNetSvc] }
    condition: all-of         # across the where entries; any-of also allowed
    min_count: 1              # matching ROWS in one file

  # Detection form: Sigma's shape — named selections and a condition over them.
  - id: sigma-style
    title: Service install that is not the benign one
    severity: high
    prefix: lakehouse
    table: logs.events
    detection:
      selection:                                # a selection is an AND of its entries
        - { column: provider_name, equals: ["Service Control Manager"] }
        - { column: event_id,      equals: [7045] }
      filter:
        - { column: service_name,  contains: [benign] }
      condition: selection and not filter       # and | or | not | 1 of | all of | ( )

  # Window form: a rate. N matching rows for one key inside a timeframe,
  # counted ACROSS files.
  - id: brute-force
    title: Repeated login failures for one account
    severity: high
    prefix: lakehouse
    table: logs.events
    where: [{ column: outcome, equals: [failure] }]
    window:
      ts_column: event_time     # which column carries event time; never guessed
      timeframe: 5m             # 500ms | 30s | 5m | 2h | 1d
      group_by: [account]       # optional; absent counts every matching row together
      count: 5                  # at least 2 — one is a rule without a window
```

A rule declares exactly one of `column:`, `where:` or `detection:` — carrying two is refused, not
merged — and `column:` with `match`/`contains` is exactly the one-column case of the same field
list.

**Quote anything that looks like a boolean.** YAML reads `no`, `off`, `yes` and `true` as
booleans, so `equals: [no]` would silently match the text `false` — a rule that loads clean,
reviews as correct, and never fires. kahshe refuses it and says how to fix it; write
`equals: ["no"]`. A frontend emitting rules should emit JSON, which has no such ambiguity and
which the loader reads through the same path.

**Loading is fail-open per rule.** That refusal is of the one rule, not the file: a WARN naming it
and the reason, `kahshe_watch_rules_skipped_total` incremented, and the rest of the file loads. A
missing `id` or `prefix`, a bad `severity`, a malformed `table`, an unknown field, an `re` that
will not compile and an id repeating an earlier rule's (first occurrence wins) all take that path,
so a deploy can lose one rule and keep watching. Only a file that will not parse at all is refused
whole, and then the previous rule set keeps serving. Alert on `kahshe_watch_rules_skipped_total`,
or compare `kahshe_watch_rules_loaded` against the rule count you shipped.

---

## 3. Field operators

Inside a `where` entry or a selection. Values within one entry are OR'ed.

| operator | meaning |
|---|---|
| `match` | analyzer tokens, exact — the value tokenizes to a list holding this token. Validated at load through the tokens contract, so a needle that is not one indexable token is refused loudly |
| `contains` | case-insensitive substring, 3 characters or more |
| `equals` | the whole value: numeric on a numeric column (`7045`, `"07045"` and `7045` in the data are one literal), exact and case-sensitive on a string |
| `equals_ignore_case` | the whole value, case-insensitively. The rest of the text vocabulary is already case-insensitive, so this is the one to reach for unless case is part of the detection; the numeric equivalence is unchanged, because the value is canonicalised before it is folded |
| `starts_with` | case-insensitive prefix |
| `ends_with` | case-insensitive suffix |
| `re` | a regular expression (Java syntax), case-sensitive, found anywhere in the value; refused at load if it does not compile |
| `gt`, `gte`, `lt`, `lte` | numeric comparison. The rule's value must be a number; a row whose value is not one satisfies no comparison, rather than failing |

Only `match` and `contains` ride the index build, and only on one column, unwindowed, as one field
or an `any-of` over several (`WatchRule.ridesIndex`) — `all-of` over more than one field is the row
scan's, because the index can only say a token is *somewhere* in the file, and OR commutes with
"some row has" while AND does not. Every other operator — `equals`, `equals_ignore_case`, `starts_with`,
`ends_with`, `re` and the numeric comparisons — and any `not` or nested condition fires through the
row scan alone. `equals_ignore_case` is where that bites: it is what a converted Sigma rule maps to
by default, and the term index stores case-preserving canonical forms, so answering it from the
index would need a second analyzer. `KAHSHE_WATCH_SCAN=false` therefore silences every rule outside
the `match`/`contains` shape, not just the regex and numeric ones.

### List and map columns

An operator on a `list<T>` or `map<K,V>` column is applied to **each member on its own**, and the
condition is satisfied if **any** member satisfies it. A map is read through its **values**, the
same half the index writes. Members are never joined into one string, so a rule sees the values
that are there and not Java's rendering of the collection:

```yaml
# tags = ["alpha", "bravo"]
- { column: tags, contains: [alpha] }        # fires — a member contains it
- { column: tags, equals:   [bravo] }        # fires — a member IS it
- { column: tags, contains: ["alpha, bravo"] }   # does NOT fire: no value contains that
```

That is the same existential the index answers ([FORMAT.md §6.7](FORMAT.md)), which is what keeps a
rule meaning the same thing whether it rode a build or a scan.

**The quantifier is per condition, and two conditions on one container column are refused.** Each
asks "some member satisfies me", so two of them ask *some member matches A, and some member matches
B* — while the rule reads, and its confirmation SQL would claim, *one member matches both*. They
differ exactly when the literals match different members:

```yaml
# REFUSED when the row scan opens a file on this table — the loader has no
# schema — with the reason and the fix in the WARN
- { column: cmd_argv, contains: [powershell] }
- { column: cmd_argv, contains: ["-enc"] }
# would fire on cmd_argv = ["/bin/sh -c echo powershell", "notepad.exe -enc AAA"],
# where neither member is the command the rule describes
```

Put the values in one condition if **any** of them may match
(`{ column: cmd_argv, contains: [powershell, "-enc"] }`), or name a scalar column for the part that
must hold of the same value. The refusal is conservative — it does not read the condition tree to
work out which pairs are genuinely AND'ed. It happens on the scan, not at load: the rules file has
no schema to say which columns are containers, so the rule loads clean and counts on
`kahshe_watch_rules_loaded`, and the row scan skips it with a WARN each time it opens a file on
that table, without touching `kahshe_watch_rules_skipped_total`. Under `any-of` on an indexed
column the index-riding path still answers it, which is sound — a disjunction of existentials is
what the rule reads as.

---

## 4. The condition, in the `detection` form

Every key but `condition` names a selection, whose entries are AND'ed. The condition is an
expression over those names: `and`, `or`, `not`, parentheses, and the quantifiers `1 of` and
`all of` over `them` or a name pattern (`1 of selection_*`).

Two things are **refused** rather than warned about, because both are typos in practice: a
condition naming a selection that does not exist, and a selection the condition never uses.

**`not` and nulls.** A row whose column is null does not satisfy a field, so `not` of that field
holds and the row fires. The confirmation SQL says the same thing: a negated term is emitted as
`NOT COALESCE(..., false)`, because SQL's `NOT NULL` is NULL and a bare `NOT` would return fewer
rows than the alert counted.

---

## 5. Window rules

> **Window rules are a prototype, and lab-run.**

They are the one rule shape that needs state across files — five failures in five minutes
routinely arrive in three files, so a per-file count silently undercounts — and that state has
costs the other paths do not, all of them counted rather than assumed.

**A window rule makes the scan sequential for that table**, and reads its files in ascending order
of the time column, because a window counts across rows and concurrent reads interleave event
time between threads. Measured on http_logs: four threads dropped 36,861 events as unplaceable and
raised 1,941 fewer alerts than one thread, about 3% of detections lost. The cost is the
parallelism, on that table only, and only when a rule asks for it.

| what | metric | why it matters |
|---|---|---|
| a threshold tripped | `kahshe_watch_window_trips_total` | the alerts |
| a key evicted at the memory cap | `kahshe_watch_window_key_evictions_total` | a partial window thrown away — **a missed detection** |
| an event older than a full buffer | `kahshe_watch_window_late_drops_total` | late data that could not be placed — also a miss |
| live keys | `kahshe_watch_window_keys` | what the cap is holding back |

`KAHSHE_WATCH_WINDOW_MAX_KEYS` (200,000) bounds the keys one rule tracks; measured at **186 bytes
per key** at `count: 5`, so the default is ~37 MB per rule and a million keys is ~178 MB. The
counting itself is not the cost — 4.6M events/s on one thread, 8.0M on four, against a Parquet
read that delivers far fewer rows than that, and only rows that MATCH the rule reach it at all.

A rule declaring `window:` is answered by the window scanner **alone** — never also as a plain
match rule, which would fire on the first matching row and turn a rate into a match. `min_count`
and `window` in one rule are refused for the same reason: one counts within a file, the other over
time.

### A window alert's confirmation SQL

The grouped count, on the whole table rather than one file. This is where Iceberg's own min/max
pruning pays: on the lab's http_logs (247M rows) it answers in **2.0 s**, because the time bound
lets the engine read the window's files and no others.

```sql
SELECT "clientip", count(*) AS n FROM iceberg."logs"."httplogs"
FOR VERSION AS OF 483670635571079646
WHERE "ts" BETWEEN TIMESTAMP '1998-06-12 11:11:53.000'
                AND TIMESTAMP '1998-06-12 11:11:54.000'
  AND ("status" = 404)
GROUP BY "clientip" HAVING count(*) >= 3
```

The bound is a **literal chosen from the time column's type**, and the reason is worth stating
because it is not obvious: an expression like `from_unixtime(...)` returns `timestamp with time
zone`, so against a zone-less `timestamp` column the engine coerces the column into the *reader's*
session zone and the window moves with whoever runs the query — the same alert confirms in London
and returns nothing in New York, which reads as a false positive rather than as a broken query.

So a zone-less column gets a wall-clock literal, a `timestamptz` column the same instant marked
`UTC`, a date column a `DATE` literal, an integral epoch column the raw millis, and a string column
ISO-8601 text. The column is never wrapped, so pruning still applies. Verified against Trino under
`UTC`, `America/New_York`, `Asia/Tokyo` and `Australia/Sydney` — same key, same count, 2.0 s each.

### Surviving a restart: replay, not a checkpoint

A window half-counted when the process died must not be lost, so the first time a process sees a
table it re-reads the files committed within the longest timeframe any window rule on that table
asks for, and rebuilds the counters from them (`kahshe_watch_replay_files_read_total`). There is
no checkpoint file: the data files **are** the durable state, so there is no second format to
write, version or garbage-collect, and no window in which the checkpoint is behind the data.
`KAHSHE_WATCH_REPLAY_MAX_FILES` bounds it.

What replay cannot rebuild is which windows already **alerted**. A window that fired shortly
before a restart fires again, carrying `"replayed": true` — a duplicate rather than a miss, which
is the direction this system chooses every time. The payload carries `window_key` and
`window_end_ms`, so a receiver that cares dedupes on (rule, key, window end). Replay is only for
the scanners that hold state across files: a rule without a window is never replayed and stays
strictly prospective.

---

## 6. Alerts

### Confidence semantics

Row-scan evidence is **exact at the row**: the scan read the value itself, and the alert carries
the matching row count and the first few matching row positions per field. The one exception is a
snapshot carrying delete files, where the same evidence is labelled `advisory` and `matched_rows`
is an upper bound — [Merge-on-read tables](#8-merge-on-read-tables), below.

Index-riding evidence is file-granular: `match` is *exact at file granularity* (the term index
holds true per-file token counts) **on a snapshot proven to carry no delete files**, and
*advisory* otherwise — the build reads raw data files and applies no delete, so its counts are
upper bounds exactly as the row scan's are. `contains` is always *advisory* (the literal's grams
all occur in the file's gram set, which strongly suggests — but does not prove — the substring).

### `confirmation_sql`

Every alert carries one, in Trino dialect, snapshot-scoped via `FOR VERSION AS OF`. A per-file
alert's SQL is pinned to the file via `"$path"`; a window alert's is not — a window's rows are
spread across files, which is why the rule needed state at all — so it carries the time bound and
the grouped count instead.

A row-scan rule's SQL carries **every** field's predicate, joined by the rule's condition: `=` for
`equals` (unquoted on a numeric column), `lower(CAST(... AS varchar)) = ...` for
`equals_ignore_case` on a text column and plain numeric equality on a numeric one (a number has no
case to fold, and folding `07045` as text would match nothing the scan matched),
`position(...) > 0` for `match` and `contains`, `LIKE ... ESCAPE '\'` for the prefix and suffix
operators, `regexp_like(...)` for `re`, and a bare comparison for `gt`/`gte`/`lt`/`lte` (wrapped in
`TRY_CAST(... AS DOUBLE)` on a non-numeric column, so a row whose text is not a number is excluded
rather than failing the query). A negated term is `NOT COALESCE(..., false)`.

### Delivery is at-least-once

Alerts are deduplicated by (rule id, file path) in a bounded in-memory cache — restarts or
eviction can re-alert, and compaction rewrites data files under new paths, which re-alerts on old
content. Full rebuilds of files the previous index already covered are suppressed (counted in
`kahshe_watch_suppressed_total`) so a rebuild never re-fires the whole table;
`KAHSHE_WATCH_REALERT_ON_REBUILD=true` opts out, with such alerts marked `re_observation: true`.

Every alert is a structured WARN log line, a counter increment, and — when configured — a JSON
POST to `KAHSHE_WATCH_WEBHOOK`: single drain thread, bounded queue, retries, and a drop counter.
Webhook failure never affects builds or serving.

**Delivery is a seam.** `KAHSHE_WATCH_SINK` picks one `AlertSink` at startup from the providers
found on the classpath through `java.util.ServiceLoader`, so a sink kahshe does not ship joins by
adding a jar — no edit to the app and no fork. An unknown name fails startup rather than delivering
nowhere. Three are built in:

- `webhook` — the default.
- `log` — one INFO line per alert carrying the payload JSON, nothing to drain, for a dev stack that
  wants to see alerts without standing up a receiver.
- `none` — delivers nothing, on purpose. Alerts still count on `kahshe_watch_alerts_total` and
  still log, so a process running it is evaluating rather than silent. This is what an
  indexer-fleet member runs ([OPERATIONS.md §4](OPERATIONS.md#4-deployment-shapes)), so one watcher
  elsewhere delivers from their build reports and alert dedup stays in a single process. It is not
  the same as leaving `KAHSHE_WATCH_SINK` unset, which means the webhook: with no URL that one is
  built disabled and discards each alert without even counting a drop. Both read `enabled=false` in
  the startup line; only `none` says the silence was meant.

### Security note

Webhook payloads carry table paths, matched terms and counts — content-revealing metadata. Point
the webhook only at trusted collectors, and keep `KAHSHE_WATCH_WEBHOOK` and
`KAHSHE_WATCH_WEBHOOK_AUTH` in Secrets, not ConfigMaps.

---

## 7. Sigma rules convert

An existing Sigma library runs against a kahshe table without being rewritten:
[`pysigma-backend-kahshe`](../sigma/README.md) compiles Sigma YAML into the rules above —
selections, the condition expression, the field modifiers — and an `event_count` correlation into
a window rule.

What it will not do is drop a clause. Every construct kahshe has no operator for is **refused**:
the conversion raises `KahsheConversionError` naming the construct and, where one exists, what to
write instead, because a rule that converts with a clause silently dropped loads clean, reviews as
correct, and fires on the wrong rows. The conversion is gated by a round trip — the backend's tests
write their output and kahshe's own loader reads it back — which is what catches YAML kahshe would
refuse.

---

## 8. Merge-on-read tables

The row scan reads a data file's raw rows and applies no delete file. On a merge-on-read snapshot
it therefore sees rows the engine no longer returns, so **`matched_rows` is an upper bound** and
the evidence says `advisory` rather than `exact`.

The confirmation SQL does apply the deletes, so it comes back with **fewer** rows than the alert
claimed. That is the deletes, not a false positive — and the distinction is why the evidence is
labelled rather than the alert suppressed: a detection that goes silent on a merge-on-read table
is worse than one that over-counts and says so.

**The index-riding path is the same hazard on the other road.** An index build also reads raw data
files and applies no delete, so its per-file term counts are upper bounds on a merge-on-read
snapshot, and `match` evidence from them says `advisory` there too. `WatchEngine` cannot read a
snapshot itself — the watcher's evaluation path names no table format, only its reader and catalog
pollers do, and a test enforces it — so it takes the verdict through a `Deletes` seam that `app`
answers from the catalog (`SnapshotDeletes`). An engine wired without one treats every snapshot as
unproven and labels everything advisory.

The decision is read once per pass (or once per build) from the snapshot summary's
`total-delete-files`, the same key the plan path refuses on. A summary that does not *prove* the
count is zero is treated as delete-bearing; `kahshe_watch_scan_delete_bearing_files_total` counts
the files affected on the scan path and `kahshe_watch_build_delete_bearing_total` the builds on the
index-riding one that carried a `match` rule — a `contains`-only build is already advisory and is
not counted. A non-zero value means some alerts over-count, not that any were missed.

---

## Further reading

- [../watch/README.md](../watch/README.md) — why the module has this shape
- [../sigma/README.md](../sigma/README.md) — the pySigma backend
- [ARCHITECTURE.md §8](ARCHITECTURE.md#8-what-the-watcher-does) — the watcher inside the system
- [CONFIGURATION.md](CONFIGURATION.md) — the `KAHSHE_WATCH_*` variables
- [OPERATIONS.md §4](OPERATIONS.md#4-deployment-shapes) — where the watcher runs
