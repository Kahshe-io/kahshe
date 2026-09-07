# kahshe endpoints

Everything kahshe does not serve itself is forwarded to the backing catalog unchanged. What it
serves is a small surface: the Iceberg REST spec's server-side scan-planning endpoints, three
filter extensions on top of the standard grammar, one non-spec count endpoint under its own
`/kahshe/` path, and the admin port.

---

## 1. Standard Iceberg REST

Transparently proxied, with these modifications on the way through:

- `/v1/config` gains the scan-planning endpoints in its `endpoints` list.
- `/v1/config` **loses** any `uri` the backend advertises under `defaults` or `overrides`. Clients
  honour it as a redirect for every later request — Nessie advertises its own address — so leaving
  it in place would route the client around the proxy after the first config call
  (`Mutations.mergeConfigEndpoints`, logged at INFO when it strips one).
- A backend that advertises **no** `endpoints` list at all gets a synthesized baseline of the 23
  standard catalog and view endpoints, plus the plan endpoints. A client that sees the key trusts
  only what is in it, so an injected list has to cover the surface the backend was serving
  implicitly.
- `LoadTableResponse` gains `scan-planning-mode=server`, which is what makes a stock Iceberg
  1.11+ client switch to server-side planning with no client configuration
  (`KAHSHE_INJECT_PLANNING=false` turns the injection off).

`planTableScan`, plan-status, fetch-tasks and cancel are served locally. A snapshot carrying
delete files is **not** advertised or served by default — the client plans locally for it,
correct and unaccelerated (`KAHSHE_SERVE_DELETE_BEARING`).

Every plan is answered `COMPLETED` inline with all of its tasks, so no id names server-side state
and the codes follow from that. Errors are `{"error":{"message":…,"type":…,"code":…}}`, `type`
naming the Iceberg exception where there is one:

| code | `type` | when |
|---|---|---|
| 200 | — | submit |
| 204 | — | cancel of an id kahshe minted (they start `sync-`) — nothing was created, so nothing is left to cancel |
| 400 | `BadRequestException` | unreadable request, or a filter extension under `OR`/`NOT` |
| 401 / 403 | `NotAuthorizedException` / `ForbiddenException` | no `Authorization` header, or the caller's own token cannot load the table — a denial the backend itself produced is relayed verbatim, under its own names |
| 404 | `NoSuchPlanIdException` | every fetch, every fetch-tasks, and a cancel of any other id — the normal answer rather than a fault, since the tasks came back with the submit |
| 404 | `NoSuchTableException` | no such table |
| 413 | `PayloadTooLarge` | body over `KAHSHE_MAX_BODY_BYTES` |
| 422 | `UnprocessableEntityException` | the snapshot is not provably delete-free — a delete landed between the client's loadTable and its plan, and a plan response has no way to say "plan this one yourself" |
| 500 | `InternalServerError` | anything unhandled; the exception text stays in the log |

If the response rewrite fails for any reason, the request passes through unmodified and the
client sees its own catalog: `kahshe_response_rewrite_failures_total`.

---

## 2. Filter extensions

Three predicates the standard grammar has no spelling for. All three are accepted in **conjunctive
positions only** — under `OR` or `NOT` they are a 400, not a silent pass.

### `contains`

```json
{"type": "contains", "term": "msg", "value": "disk pressure"}
```

Case-insensitive substring. Served by the exact gram layer where it covers the file, with the
blooms behind it for files it does not.

### `match`

```json
{"type": "match", "term": "msg", "value": "offilialog"}
```

Token equality under the analyzer the index names — `kahshe-ascii-v3-max<cap>`, or the earlier
`kahshe-ascii-v2-max<cap>` and `kahshe-ascii-v1` read under their own rules, or
`kahshe-value-v1-max<cap>` for a whole-value column. Served from the term dictionary, and never
derived from `LIKE`.

Under analyzer v3 a compound identifier such as `10.0.4.17` is one term, so `match` on it finds
the rows that hold the address rather than the rows holding each octet.

This is the one extension the Trino overlay reaches on its own. It recognises the exact pattern
`Analyzer.matchPattern` writes, under `lower(col)` — the plain-token boundary, or this compound
form when the needle carries separators:

```
(^|[^a-z0-9.:_-])[.:_-]*10\.0\.4\.17[.:_-]*([^a-z0-9.:_-]|$)
```

Only that shape; anything else is left alone and merely costs a scan. It reaches kahshe as the
sentinel spelling below, not as a `match` node.

### `match_prefix`

Every **term** starting with the value — a subnet inside a log line, a UUID prefix — as a range
of the dictionary, capped by `KAHSHE_PREFIX_MAX_TERMS`. No engine overlay emits it: it arrives
through the filter extension or the expressions-spec form only.

### The expressions-spec form

All three are also accepted in the merged expressions spec's form (apache/iceberg
`format/expressions-spec.md`, Appendix B). A boolean function is not a predicate there, so it
arrives compared:

```json
{"type":"eq",
 "left":{"type":"apply",
         "function":{"catalog":"kahshe_functions","identifier":["match"]},
         "arguments":[{"type":"reference","name":"msg"},"needle"]},
 "right":true}
```

The column is referenced by `name` (a scan-planning client) or by `id` (a stored expression); the
deprecated `term`/`value` spelling and `literal` objects are read too. Only `eq … true` is a
hint — any other comparison of an `apply` goes to the standard parser and is refused there. The
function catalog must be named `kahshe_functions`; the reserved `iceberg_functions` namespace is
still honored on read for older callers, never emitted. In this form only, `text_match` is read as
an alias of `match`; the direct node form above takes the three names and nothing else.

### The sentinel spelling

The encoding the Trino overlay actually emits, because iceberg-java 1.11.0's `ExpressionParser`
cannot serialize `apply`. The predicate rides as an ordinary `eq` on a column name that does not
exist:

```json
{"type":"eq","term":"__kahshe_match__msg","value":"offilialog"}
```

`__kahshe_match__<column>` is a `match` hint and `__kahshe_contains__<column>` a `contains` hint;
there is no prefix sentinel. Only `eq` is accepted on one — another operation means the producer
and the reader disagree about the encoding, and guessing would drop a predicate the engine
believes it pushed. A table that really has a column of that name wins: the sentinel is not read
as a hint when the name is a real column, since nothing reserves the prefix. `ContainsExtractor`
strips the sentinel before anything binds the filter, which is why the overlay adds one only for a
scan the catalog is planning — a local planner would bind it against the schema and fail the query
outright.

### What reaches the index from plain SQL

No filter extension is needed for these — they arrive as ordinary predicates and reach the
dictionary on their own:

| SQL | tier |
|---|---|
| `=`, `IN` on any indexed column | term dictionary, via the canonical string form |
| `LIKE 'x%'` on a `value`-analyzer string column | dictionary prefix range |
| `>=`, `<`, `BETWEEN` on a `value`-analyzer string column | dictionary range |
| bounded predicates on any column | Iceberg's own min/max statistics, first, before kahshe |

Token `match` intent against a `tokens` column is the gap: no shipped SQL syntax expresses it, so
it needs the filter extension or the Trino overlay.

---

## 3. `_count`

```
POST /kahshe/v1/{prefix}/namespaces/{ns}/tables/{table}/_count
{"column": "msg", "term": "offilialog"}
{"column": "msg", "prefix": "10.0.4."}
```

Exact token counts from the aggregate term layer. A `prefix` form sums every term under the
prefix, under the same exactness rule.

**`count` is token occurrences** — what Lucene calls `total_term_freq` — **not rows**: a row
holding the token twice counts twice, unlike SQL `count(*)`, Splunk `tstats count` or
Elasticsearch `_count`. A revision to row counts is planned; the endpoint has no SQL surface
today, so nothing depends on the current meaning.

It **refuses rather than approximates**: deletes present, stale coverage, a multi-token value, or
**any data file having left the table since the index was built** — an occurrence count is a
scalar sum, so a departed file's occurrences cannot be subtracted from it, and a full rebuild
restores exactness. Pruning is unaffected by that last case: it only asks which files hold a
term, never how many times.

Its own errors carry one `type`, `CountError`, rather than Iceberg exception names; the 413 and
500 in §1 come from the server ahead of it and keep theirs.

| code | when |
|---|---|
| 200 | the count — also `0` for a table with no snapshot |
| 400 | unreadable body, or one naming neither or both of `term` and `prefix` |
| 404 | no such column, or that column has no term index |
| 422 | each refusal above, and: a prefix no term under this analyzer could start with, a term it never indexed, a build still in flight over part of the files, a prefix matching more terms than `KAHSHE_PREFIX_MAX_TERMS` |
| 503 | the term index cannot be read — which is not a count of zero |

`_count` is a content oracle at table-level authorization:
[OPERATIONS.md § Security posture](OPERATIONS.md#6-security-posture).

---

## 4. Admin port (`KAHSHE_ADMIN_PORT`, default 8283)

`GET /healthz`, `GET /readyz`, `GET /metrics` (Prometheus text), on their own executor so probes
keep answering while the data plane is saturated. The metric reference is in
[OPERATIONS.md](OPERATIONS.md).

---

## Further reading

- [CONFIGURATION.md](CONFIGURATION.md) — every variable and table property
- [OPERATIONS.md](OPERATIONS.md) — metrics and what to alert on
- [ARCHITECTURE.md](ARCHITECTURE.md#3-the-request-path) — how a request becomes a file list
- [FORMAT.md](FORMAT.md) — what the tiers behind these answers store
