# `proxy` — the Iceberg REST surface

The HTTP module an engine actually talks to: it implements Iceberg's server-side scan-planning
endpoints on top of kahshe's indexes, and forwards everything else to the catalog you already run.

An engine configured against kahshe sees an ordinary Iceberg REST catalog. `GET /v1/config` and
`loadTable` responses come from the backing catalog with two edits — the scan-planning endpoints
are advertised, and `scan-planning-mode=server` is injected — which is enough for an Iceberg 1.11+
client to start sending its filters here instead of planning locally. When it does, this module
plans from manifests, evaluates the filter against per-file statistics, hands the surviving files
to the index pruner, and returns the file-scan tasks that can match. Nothing else in the REST
surface changes, and no engine configuration beyond the catalog URI is required.

Artifact `kahshe-proxy`, package `io.kahshe.proxy`. It sits near the top of the
[dependency stack](../docs/ARCHITECTURE.md): it depends on `indexer` (serving traffic is what drives
index maintenance), on `format` for the pruner and the term reader, on `analysis` and `common`, and
on Iceberg's own REST catalog client. It never depends on `watch`, and `watch` never depends on it —
the two roles share only the layers below both. Nothing in the repository consumes this module
except `app`, which wires it to an HTTP server.

---

## The surface

| Route | Handled by | Behaviour |
| --- | --- | --- |
| `POST /v1/{prefix}/namespaces/{ns}/tables/{t}/plan` | `PlanRoutes` → `PlanService` | Planned here. Always `COMPLETED`, with every task in the response. |
| `GET  …/plan/{plan-id}` | `PlanRoutes` | Always 404 — see [Inline plans](#inline-plans-and-plan-ids). |
| `DELETE …/plan/{plan-id}` | `PlanRoutes` | 204 for an id this proxy minted, 404 otherwise. |
| `POST …/tasks` | `PlanRoutes` | Always 404; no plan is ever paged. |
| `POST /kahshe/v1/{prefix}/namespaces/{ns}/tables/{t}/_count` | `CountRoutes` | kahshe extension: exact term and prefix counts from the term index. |
| `GET /v1/config` | `Forwarder` + `Mutations` | Forwarded; endpoint list merged, backend-advertised `uri` stripped. |
| `GET /v1/{prefix}/namespaces/{ns}/tables/{t}` | `Forwarder` + `Mutations` | Forwarded; `scan-planning-mode=server` injected when the table qualifies. |
| everything else | `Forwarder` | Forwarded verbatim, response relayed with its headers. |

`AdminHandler` serves `/healthz`, `/readyz`, `/metrics` and `/index` on a **separate port with its
own executor**, so probes and scrapes keep answering while the data-plane thread pool is saturated
by a slow backend. No admin request ever calls the backend inline: a scheduled prober publishes a
reachability verdict and `/readyz` reads it. `AdminAuth` gates everything but the two probes behind
`KAHSHE_ADMIN_TOKEN` when one is set.

## How a request moves

```
                       ┌──────────── KahsheHandler (dispatcher) ────────────┐
  engine ── HTTP ──▶   │ body cap → route match                            │
                       └───┬───────────────┬───────────────────┬───────────┘
                           │ /plan         │ /_count           │ everything else
                           ▼               ▼                   ▼
                       AuthGate        AuthGate            Forwarder ──▶ backing catalog
                           │               │                   │
                           ▼               ▼                   ▼
                       PlanRoutes      CountRoutes         Mutations (rewrite the response)
                           │               │                   │
              ContainsExtractor            │            MutatedTables (which tables changed)
                           │               │                   │
                           ▼               ▼                   ▼
                       PlanService     TermIndex          BackendCatalogs (invalidate / observe)
                           │
                    IndexPruner (format module)
```

**Types worth knowing.**

- **`KahsheHandler`** — the single `HttpHandler` for the data plane. Reads the body under a cap,
  matches the path, and dispatches. On the passthrough path it is also where the response rewrites
  and the cache invalidations happen.
- **`Forwarder`** — a thin verbatim relay to the backing catalog. Skips hop-by-hop and recomputed
  headers in both directions.
- **`AuthGate`** — caller authorization for the two endpoints kahshe answers itself.
- **`PlanRoutes`** — path matching, request/response JSON, and every error mapping for the four
  planning endpoints.
- **`ContainsExtractor`** — lifts kahshe's text predicates out of the filter before Iceberg's own
  parser sees it, returning the standard JSON plus a list of pruning hints.
- **`PlanService`** — the plan itself: manifest scan, stats evaluation, index pruning, response
  assembly, and the plan cache.
- **`CountRoutes`** — the `_count` extension.
- **`BackendCatalogs`** — the Iceberg `RESTCatalog` clients under both identities: one per (caller
  token, prefix) for the served endpoints, the default, and one service client per path prefix for
  builds and the `service` opt-in; the one place the identity is chosen (`forPlanning`); the table
  caches; and the record of what has been forwarded to clients.
- **`Mutations`** — the JSON rewrites applied to `/v1/config` and `loadTable` responses, and the
  readers for the `kahshe.index` property and the current snapshot id.
- **`MutatedTables`** — reads the tables changed by the two spec endpoints that name them in the
  body rather than the path.
- **`ProxyConfig`** — the serving-side configuration record. Nothing in it is read by an index
  build or by a reader of an index artifact.

## Seams

**`io.kahshe.indexer.TableSource`** — implemented here by `BackendCatalogs`. It is the one seam
between index maintenance and whatever catalog a deployment fronts: `load(prefix, ident)` and
`invalidate(prefix, ident)`. Implementing it lets you drive kahshe's indexer from a catalog that is
not this proxy — a catalog server embedding the indexer implements it over its own `Catalog`
instance.

**The filter extension protocol** — `ContainsExtractor` is the client-facing seam. A client that
wants substring, token or prefix pruning has three ways to say so, and all three arrive as ordinary
JSON inside a standard `planTableScan` request:

| Form | Shape | When to use it |
| --- | --- | --- |
| Function application | `{"type":"apply","function":{"catalog":"kahshe_functions","identifier":["contains"]},"arguments":[{"type":"reference","id":5},"needle"]}`, bare or compared `eq`-to-`true` | A client that can emit the expressions spec's function-application form. |
| Node type | `{"type":"contains","term":"msg","value":"needle"}` (also `match`, `match_prefix`) | A client writing the filter JSON directly. |
| Sentinel term | `Expressions.equal("__kahshe_match__msg", "token")` | A client stuck behind an expression builder that cannot emit an `apply` node. It is an ordinary `UnboundPredicate`, so every engine serializes it unchanged. |

`ContainsExtractor.FUNCTION_CATALOG` is `kahshe_functions`, deliberately not the reserved
`iceberg_functions` namespace: reading kahshe's predicates from a namespace it does not own would
claim a name it has no right to and collide the day Iceberg defines a function of the same name with
different semantics. The reserved spelling is still accepted for compatibility.

**Index types and analyzers** are `ServiceLoader` seams, but they belong to the `format` and
`analysis` modules — this module consumes `IndexPruner` and `TermIndex` and adds no discovery of its
own. A new index type joins the plan path by registering there; nothing in `proxy` needs to change.

**Configuration** is read from the environment by the application module and handed here as
`ProxyConfig` and `FormatConfig`. The knobs this module reads:

| Variable | Effect |
| --- | --- |
| `KAHSHE_BACKEND` | Base URL of the catalog being fronted. |
| `KAHSHE_BACKEND_CA` | PEM bundle of the CA that signed the backend's certificate, for a catalog behind a private authority. `BackendTls` gives it to every client that reaches the backend — Iceberg's `RESTCatalog` under both identities, service and caller, and the passthrough `Forwarder` — rather than to the JVM default trust store, which would also change how object storage is trusted. |
| `KAHSHE_CREDENTIAL`, `KAHSHE_SCOPE` | OAuth2 client credentials for the service-identity catalog clients: what a build reads with, and served reads only under `service`. |
| `KAHSHE_BACKEND_WAREHOUSE` | Overrides the guess that a path prefix names the warehouse. |
| `KAHSHE_PLANNING_IDENTITY` | `caller` (default) or `service`: whose client reads a table's metadata on the served endpoints, and the identity the plan cache is keyed by. |
| `KAHSHE_INJECT_PLANNING` | Whether `scan-planning-mode=server` is injected at all. |
| `KAHSHE_SERVE_DELETE_BEARING` | Escape hatch for delete-bearing snapshots; off by default. |
| `KAHSHE_PLAN_STATS` | `strip` (default) or `requested`. |
| `KAHSHE_TABLE_CACHE_TTL_MS` | Table-cache TTL, default `10000` (10 s); `0` disables caching entirely, which is what more than one replica requires. |
| `KAHSHE_AUTH_CACHE_TTL_MS` | How long a positive authorization verdict is memoized. |
| `KAHSHE_BACKEND_TIMEOUT_MS`, `KAHSHE_MAX_BODY_BYTES` | Request bounds. |
| `KAHSHE_PREFIX_MAX_TERMS` | Ceiling on the terms a prefix count may expand to. `CountRoutes` names it in the 422 it returns when a prefix is too broad, so a caller who hits that limit is sent straight here. |

Two table properties are read from the catalog rather than the environment: `kahshe.index` names the
columns to index, and `kahshe.plan-stats` overrides `KAHSHE_PLAN_STATS` for one table.

---

## Design rationale

### Inline plans and plan ids

Every plan — full, time-travel and incremental alike — is answered `COMPLETED` with all of its
tasks in the same response. No plan id ever names server-side state.

This is what makes the proxy horizontally scalable without a shared store. Iceberg's reference
implementation parks overflow batches in a JVM-global map, so a client whose follow-up fetch lands
on a different replica gets a 404 and reports a failed query. Answering inline removes the
possibility: there is nothing for a second request to find, so there is nothing for a second replica
to be missing. The cost is that a plan response is proportional to the table's file count rather
than paged.

The endpoints still exist, because the spec says they do, and their behaviour follows from the
above. Fetch is always 404. Cancel is 204 for an id carrying the prefix `PlanService` mints and 404
otherwise — decided from the id's *shape* alone, never by consulting a record. That last point is a
security property, not an implementation shortcut: Iceberg resolves plan ids from a global table with
no table binding, and the auth gate only proves the caller may load the table named in the *path*.
If ids were checked against a record, a 404 would confirm which ids are real to a caller who is not
authorized on the table they belong to. So the refusal is uniform and indistinguishable.

### Two-pass planning, and per-file statistics

`PlanService` runs the manifest scan twice against the same pinned snapshot: once with column
statistics for evaluation, once without them for the response. Both lists are cached together under
the table's location and carry the snapshot they were built from, so a snapshot change is a miss
rather than a stale hit. Concurrent cold plans for one table share a single manifest read.

The reason is disclosure. Per-file column min/max bounds are a content oracle: a caller who may list
a table's files learns approximate values in every column of every file. Under table-level
authorization that is more than the caller was granted. The default (`strip`) is that no bounds leave
the proxy at all.

A deployment can buy the bounds back where they are not sensitive. Under `requested`, the response
carries statistics for exactly the columns a request named in `stats-fields` — which lets an engine
push filters it could not otherwise evaluate — and `kahshe.plan-stats` scopes that decision to a
single table. An unrecognized mode strips and logs: a wider disclosure is never the fallback.

If the two passes ever disagree about which files exist, the stats-laden task is used. Dropping the
file instead would remove a file the filter kept, which is a wrong answer; disclosing bounds is
merely a leak of the thing the design withholds. It is counted and logged rather than done quietly.

### The residual is withheld, always

Tasks are returned wrapped in a delegate whose `residual()` is **absent**, not `alwaysTrue`. A
spec-honoring client reads `alwaysTrue` as "no row filtering needed" and returns every row of the
file. Index pruning admits false positives by construction — a bloom filter says *may contain* — so
a client that skipped its own row filter would surface those as query results. Absent residual means
the client re-applies its filter, which is correct and costs nothing it was not already paying.

### Delete-bearing snapshots fail closed, in both halves

The REST scan-task wire format loses `DeleteFile.dataSequenceNumber`. The rule that consumes it —
`delete.seq > data.seq`, so an upsert's delete does not apply to the row it re-inserted — is applied
while *binding* deletes to data files, and that binding happens server-side, here. A reader on
Iceberg's own path therefore never consults the number: `DeleteFilter`, the class that applies
deletes to rows, does not reference it at all.

Trino is the exception, and mechanically so: `DeleteFile.fromIceberg` unboxes `dataSequenceNumber()`
with no null guard, over every entry of `task.deletes()` and ungated by content type. Against
Trino 483 that is an NPE — position deletes, equality deletes and deletion vectors alike — surfacing
to the user as an opaque `Error processing metadata for table`. Measured against a merge-on-read
table, Trino 483 fails and stock iceberg-java 1.11 returns the correct rows, on both position and
equality deletes.

So `Mutations` declines to advertise `scan-planning-mode=server` for a table whose current snapshot
is not provably delete-free, and `PlanService` refuses to plan one with a 422. Both halves are
needed and both move on the same flag: advertising without serving is a lie, and serving without
advertising is inert, because a client never told to plan server-side never asks. A table that falls
back to local planning is correct, just unaccelerated, and compaction makes it servable again.

"Provably" is literal. A snapshot whose summary does not state a delete-file count is refused as
firmly as one that states a non-zero count, because "could not prove it is delete-free" and "is
delete-free" are different claims and only one of them is safe to serve.

The refusal is broader than the one break it is known to prevent, and that is what
`KAHSHE_SERVE_DELETE_BEARING` is for: a deployment whose clients are all on iceberg-java's reader
path can lift it. It stays off by default because a plan request does not say which client sent it.

### Never plan a view older than the one you handed out

The proxy sits on the path of every `loadTable`, so it knows the snapshot each client was told
about. It records that per table, and before planning it checks whether the catalog view it is about
to plan from contains that snapshot. If not, it invalidates and reloads.

This closes a window that is otherwise completely silent. A commit that reaches the catalog without
passing through kahshe invalidates nothing; inside the table-cache TTL a plan would be built from the
pre-commit snapshot and the rows just written would simply be missing. Nothing downstream can detect
it, because a plan response names no snapshot. The check is membership, not comparison — snapshot ids
are random longs, so "older" cannot be read off the number; a history that contains the observed
snapshot is at or ahead of it.

Cache invalidation has three layers, in order of precision:

1. **Path-named mutations.** A commit or drop on a table path invalidates that table directly.
2. **Body-named mutations.** `MutatedTables` reads multi-table transactions and renames, which name
   their tables in the body and match no table path. A rename invalidates *both* ends: a drop
   followed by a rename onto the freed name would otherwise keep serving the dropped table's files,
   which is not staleness but another table's data.
3. **The TTL.** A backstop for the two cases the first two layers miss — a table this replica has
   never forwarded, and a mutating body that could not be parsed.

Identifiers are read with Iceberg's own parsers rather than by picking fields out of the JSON, so the
producer and the reader cannot drift apart. Setting the TTL to `0` disables table caching entirely,
which is the coherent choice for a multi-replica deployment: a replica that caches nothing cannot
hold a stale view.

### Authorization stays with the backend

kahshe is not an authorization system. For the two endpoints it answers itself, `AuthGate` forwards
a `loadTable` carrying **the caller's own bearer token** to the backing catalog and requires it to
succeed. The backend remains the sole authority; kahshe only memoizes the verdict.

- Cache keys are `SHA-256(token)` plus the table path. Raw credentials are never stored.
- A positive verdict lives for the configured TTL, capped at the token's own JWT expiry when one is
  parseable. Revocation lag is bounded by that TTL.
- Denials are negative-cached briefly so a bad-token storm does not amplify 1:1 into the backend, and
  they carry the status the backend actually gave. Replaying every denial as 401 would turn a backend
  brownout into a credential failure — engines abort or re-authenticate on 401, but retry a 5xx.
- Only 401, 403 and 404 are cached. A 5xx means the backend could not answer, not that the caller may
  not read the table.

The plan itself is then built as the caller by default (`KAHSHE_PLANNING_IDENTITY=caller`): a
client authenticated as the caller reads the table and its manifests, so the backend's own
authorization — including vended-credential scoping — applies to those reads too, and the plan
cache is keyed by that client, so one caller's plan is never served to another. `service` is the
opt-in, announced at startup: one shared client under `KAHSHE_CREDENTIAL` and one plan per table
for every caller the gate admits. Caller catalogs are cached by token hash, and closed on eviction,
because each one owns a pooled HTTP client and a token-refresh thread that would otherwise leak for
the life of the process.

### The body cap is enforced on bytes read

`Content-Length` is checked first, but it is not the guard. A request sent with
`Transfer-Encoding: chunked` carries no `Content-Length` at all, so a declared length of zero sails
past the check. The passthrough path reaches the body read *before* any authorization happens, so an
unbounded read would be an unauthenticated memory-exhaustion attack against every worker thread at
once. The read therefore stops the moment the running total crosses the cap, costing the cap plus one
buffer rather than however much the client chose to send.

### `_count` is exact or it refuses

`POST …/_count` answers a token or prefix count from the term index without opening a data file. It
never returns an approximation labelled exact. Every condition under which the number would be an
upper bound is a 4xx that says which condition it was:

- the table cannot be proven delete-free (422),
- the index covers a different snapshot than the table's current one (422),
- the index is a partial checkpoint, or files have left the table since it was built, so occurrence
  counts include files that are gone (422),
- the term does not analyze to exactly one token, or is not indexable under the index's own analyzer
  (422),
- a prefix matches more terms than the configured cap (422),
- the index cannot be read (503 — an unreadable leaf is not a count of zero).

Indexability is checked against the *loaded index's* analyzer contract, not the deployment default:
an index built under a larger token-length cap holds tokens the current default would reject, and
refusing them here would answer 422 for a term that is in fact counted exactly.

The response reports coverage alongside the count — files indexed, files holding the term, the
snapshot, the analyzer — so a caller can see what the number is a count *of*.

### The `uri` strip

`Mutations` removes any `uri` the backend advertises in its config `defaults` or `overrides`.
Clients honour it as a redirect for every subsequent request, so leaving it in place would route them
around the proxy after the first config call — the accelerator would appear to be installed and do
nothing. Relatedly, when a backend advertises no `endpoints` list at all, a baseline list covering the
standard catalog surface is synthesized before the planning endpoints are added, because clients trust
an advertised list exclusively.

Every rewrite fails open: an unparseable response is passed through unmodified so the client still
sees its catalog. That is correct and invisible, since the only symptom is that queries stop getting
faster — hence `kahshe_response_rewrite_failures_total`, which makes it visible instead.

---

## What this module deliberately does not do

- **It does not authorize.** No policy, no roles, no allow-lists. Every access decision is made by
  the backing catalog and merely cached here.
- **It does not build indexes.** Index construction lives in `indexer`; the proxy only *observes*
  `kahshe.index` on the loadTable it was forwarding anyway and hands the observation on. Planning
  never blocks on a build.
- **It does not read data files.** Planning touches manifests and index sidecars only. A stale,
  partial or unreadable index makes planning slower, never wrong: every file it cannot rule out is
  kept.
- **It does not hold plan state.** No plan store, no paging, no cross-request continuation, and
  therefore no shared state between replicas.
- **It does not translate SQL or rewrite queries.** It reads the filter the client already sent,
  removes kahshe's own extension nodes, and hands the rest to Iceberg's parser unchanged.
- **It does not front non-REST catalogs.** The serving path *is* an Iceberg REST catalog and speaks
  REST to the one behind it. Index maintenance is not so limited — that is what `TableSource` is
  for.
- **It does not serve delete-bearing tables by default**, and refuses rather than guessing when it
  cannot prove a snapshot is delete-free.
- **It does not decide the meaning of a term.** Analyzers and index types belong to `analysis` and
  `format`; the proxy passes a query term through the contract the index was built under and asks
  what that contract says.

---

## Tests

```sh
./gradlew :proxy:check
```

`check` runs `javadoc` under doclint alongside the suite, so a `{@link}` left pointing at a member a
rename moved fails the build.

The suite pins the properties above where they can actually break: the HTTP surface over a real
socket against a real stub backend — body cap, authorization, route matching, error mapping — and
the admin port on the single-thread executor production uses, because a wider pool would make the
starvation test pass without the handler changing. It holds the freshness rules end to end (no plan
built from a view older than the one this proxy forwarded, `KAHSHE_TABLE_CACHE_TTL_MS=0` meaning no
cache rather than a short one, a commit seen in the passthrough invalidating the caller-identity
catalogs too) and asserts that every plan is `COMPLETED` and inline, incremental scans included.
The rest cover `_count` through the route rather than the dictionary reader alone, the stats default
and its per-table override, the filter extension in all three of its forms including the request
shape Trino actually sends, and the degradations that are correct but invisible — those assert the
*counters* move, because a degradation nobody can alert on is one nobody finds.
