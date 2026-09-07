# `app` — process wiring and entry point

The one distribution: `main`, the single place the environment is read, and the wiring that turns a
set of environment variables into a running proxy, indexer and/or watcher.

Every other module in this repository is a library that knows nothing about how a process is
started. `app` is where that knowledge lives. It reads the environment exactly once into typed
configuration records, decides which roles this process runs, constructs the storage and catalog
clients the lower modules deliberately refuse to construct, starts the HTTP servers, and installs
the shutdown hook. It contains no planning logic, no index format knowledge and no rule
evaluation. `app` depends on every other module; nothing depends on `app`.

Package: `io.kahshe`. Main class: `io.kahshe.Kahshe`. The distribution installs as `bin/kahshe`.

## The types

Seven source files; `Kahshe.Config` is a record nested in `Kahshe`.

| Type | What it is |
| --- | --- |
| `Kahshe` | `main`, the role wiring, the HTTP servers, the shutdown hook, and the nested `Kahshe.Config` record that reads the environment |
| `Kahshe.Config` | the whole environment bar the TLS block, read once, partitioned into `ProxyConfig`, `BuildConfig` (which carries `FormatConfig`) and `WatchConfig`, plus the app's own fields: the two ports, the worker count, the mode, the indexer flag and the foreign catalog |
| `ServerTls` | the nine `KAHSHE_TLS_*` variables into `ServerTls.Settings`, and the `HttpsConfigurator` the servers take — including the re-read that picks up a rotated certificate |
| `WatchRoles` | a pure function from (mode, indexer flag, rules configured) to the three watch roles this process runs |
| `IndexIo` | builds the `FileIO` for an external index root from a class name and dotted properties |
| `CatalogSource` | a `TableSource` over any Iceberg `Catalog`, for a process with no data plane |
| `SnapshotDeletes` | a `WatchEngine.Deletes` over any `TableSource`: whether a snapshot carries delete files, answered from the catalog and failing closed, so the index-riding listener labels its evidence `exact` only over a snapshot proven delete-free |
| `ClientDemo` | a stock Iceberg REST client pointed at kahshe, demonstrating that no client-side configuration is needed |

## What `main` wires

```
Config.fromEnv()
   ├─ IndexPaths.externalIo(IndexIo::open)      registered before anything reads an index
   ├─ ServerTls.fromEnv()                       built before either port binds, so a bad
   │                                            certificate fails startup, not the first handshake
   ├─ BackendCatalogs (REST)  ─or─  CatalogSource (foreign)   ──▶ TableSource
   ├─ WatchRoles.of(mode, indexerOn, rulesSet)
   │     ├─ indexRiding ─▶ WatchEngine, as the IndexBuildListener the builds call
   │     ├─ discovery   ─▶ TableDiscovery + ScanPass
   │     └─ reports     ─▶ ReportPoller
   │            all three deliver through one Alerts ──▶ AlertSink
   ├─ IndexerService(tables, metrics, indexerOn, buildConfig, listener)
   ├─ Http(s)Server :KAHSHE_PORT      ─▶ KahsheHandler ─▶ PlanService | Forwarder (unless mode=watch)
   ├─ Http(s)Server :KAHSHE_ADMIN_PORT ─▶ AdminHandler        (always; TLS only under KAHSHE_TLS_ADMIN)
   └─ shutdown hook: BuildLease.releaseAll() → readiness false → drain → stop
```

Two entry paths share that configuration:

- **Server** (no arguments). Starts the roles above and blocks in the JVM's non-daemon threads.
- **CLI build**: `kahshe index <prefix> <namespace.table> <column>`. Runs one `IndexBuilder.run`
  and exits. It wires a `WatchEngine` too when `KAHSHE_WATCH_RULES` is set, because alerts follow
  builds wherever they happen, and it waits on `AlertSink.awaitDrain` before returning — an async
  sink drains on a daemon thread, and queued alerts would otherwise die with the JVM.

## Deployment shapes

`KAHSHE_MODE` selects which halves run. `KAHSHE_INDEXER` and `KAHSHE_WATCH_RULES` then decide the
rest.

| `KAHSHE_MODE` | Data plane | Watch roles when rules are set |
| --- | --- | --- |
| `proxy` | yes | the index-riding listener only, and only with the indexer on |
| `watch` | no — admin port only | discovery always, with the row scan unless `KAHSHE_WATCH_SCAN=false`; the report poller when the indexer is off, the listener when it is on |
| `both` (default) | yes | same as `watch` |

The admin port is always served, in every mode, on its own single-threaded executor.

## Seams

Four extension points, each one a class name plus properties rather than a code change.

**`AlertSinkProvider` / `AlertSink`** (in `watch`, discovered by `ServiceLoader`). Implement it,
put the jar on the classpath, and `KAHSHE_WATCH_SINK=<your name>` delivers every alert through it —
from the index-riding listener, the row scan and the report poller alike, since all three go
through one `Alerts`. An unrecognised name fails startup rather than falling back to the default,
because a typo would otherwise leave a watcher running and quietly delivering somewhere nobody
asked for.

**`FileIO` for the index root.** `KAHSHE_INDEX_IO_IMPL` names any `FileIO` on the classpath and
`KAHSHE_INDEX_IO_PROPERTIES` carries its dotted configuration; `IndexIo` hands both to
`CatalogUtil.loadFileIO`. A GCS or ADLS index root is configuration, not code. The
`KAHSHE_INDEX_S3_*` variables are a shorthand for four of the same dotted keys, and an explicit
properties entry always wins over the shorthand for the same key.

**`Catalog` for a watcher.** `KAHSHE_CATALOG_IMPL` plus `KAHSHE_CATALOG_PROPERTIES` load any
Iceberg `Catalog` — Glue, Hive, JDBC, DynamoDB, a REST catalog with its own signer — through
`CatalogUtil.loadCatalog`, and `CatalogSource` adapts it to `TableSource`. This is how the watcher
runs against a lakehouse kahshe has never seen.

**`TableSource`** (in `indexer`). The one seam between building and whatever catalog a deployment
fronts. `BackendCatalogs` and `CatalogSource` are the two implementations here; a catalog embedding
the indexer implements it over its own `Catalog` instead. **`IndexBuildListener`** (also in
`indexer`) is the matching observation seam: it is how `WatchEngine` sees files as they are indexed.

An embedder that wants none of this module can take `format` and `indexer` directly and register
its own factory with `IndexPaths.externalIo`.

## Why the code has this shape

**The environment is read once, then partitioned.** Two methods here call `System.getenv` for
configuration: `Config.fromEnv` for everything except TLS, and `ServerTls.fromEnv` for the nine
`KAHSHE_TLS_*` variables, which stay in their own record because TLS is terminated in this module
and no module below it ever sees a socket. Below `app` there is one read, `Fleet`'s of `HOSTNAME`
in `indexer`, and it is the pod's identity rather than a knob: there is no default worth threading
down for it, and the knob that overrides it, `KAHSHE_INDEX_FLEET_ORDINAL`, is read here like every
other. What comes out is not one bag of strings but a record per consumer:
`ProxyConfig` for serving, `FormatConfig` for reading and writing the index, `BuildConfig` for a
build, `WatchConfig` for alerting. A class is handed the record it needs and nothing else, which
means a test constructs the record directly and no class can quietly grow a dependency on a
variable it was never given.

**Numeric variables tolerate garbage.** Kubernetes service links inject values like
`KAHSHE_PORT=tcp://10.0.0.1:8282` into every pod when a Service shares the app's name. A process
that crashed on that would be undeployable under its own name, so `intEnv` and `longEnv` fall back
to the default on a parse failure rather than throwing.

**A few build knobs are threaded through configuration rather than read where they are used.**
Thread counts and buffer sizes could be `static final` reads at class initialization, but such a
value is pinned by the first build in the JVM and cannot be varied by a test.

**Storage clients belong to the app, not to the format.** The `format` module never constructs a
storage client; `IndexPaths.externalIo(IndexIo::open)` is registered at startup, before anything
can read an index. That keeps `format` embeddable — a catalog that wants the reader does not
inherit kahshe's opinion about which cloud SDK to load — and it is why `app` is the only module
that names `iceberg-aws`.

**Credentials are absent by default, not blank.** `IndexIo` passes static S3 keys only when both a
key and a secret are set. A blank key is not "no key", it is an invalid credential, so the client
falls back to the AWS default provider chain — IRSA, workload identity, instance profile,
environment — which is the only path that rotates without a restart.

**A foreign catalog is offered only where it can work.** A process with no data plane needs nothing
from its catalog but tables, so it can use any Iceberg `Catalog`. The proxy cannot: it *is* a REST
catalog, engines speak REST to it and it forwards everything it does not serve, so a non-REST
backend would answer half its job. `KAHSHE_CATALOG_IMPL` is therefore gated on the mode rather than
simply honoured when set, and it logs a warning when a proxy-serving process has it configured.

**A rule's prefix is not a routing key.** A prefix is a REST catalog's tenancy path segment; Glue,
Hive and JDBC have no such thing. `CatalogSource` gives every rule the same catalog, and a rule's
prefix stays part of its identity in alerts rather than selecting a client. A deployment with
several tenants runs several watchers, one per catalog — which is what the catalogs themselves make
it.

**Detection follows the rules, not the index.** `WatchRoles` exists because "does this process
watch?" and "does this process build?" are separate questions. Discovery and the row scan run
wherever rules are set and the mode has a watch role, whether or not this process builds anything.
The index-riding listener needs the indexer, because it rides this process's own builds. The report
poller is the complement: a build's per-file evidence exists only during the read pass, so a
process with the indexer off delivers the alerts that builds *elsewhere* recorded in their build
reports. Encoding this in a small pure record, rather than in a chain of conditionals in `main`,
is what makes it testable.

**Both server pools are bounded, and the admin port is separate.** The data plane runs on a fixed
pool with a 256-entry queue and `AbortPolicy`: under saturation connections are refused promptly
rather than queued without limit. The admin server gets one thread, which is enough because no
admin handler blocks — `AdminHandler` runs its backend probe on its own thread and `/readyz`
answers from the last verdict it published. Health and readiness therefore keep answering while
the data-plane pool is saturated by a slow backend.

**Shutdown releases build leases before it drains.** A build holds a lease on the column it is
writing. If the JVM dies holding one, the next process to try that column is refused for the rest
of the lease's TTL — a pod killed mid-build would otherwise block that column long after it is
gone. So `BuildLease.releaseAll()` runs first in the shutdown hook, before readiness flips and
before either server stops.

**Ambiguous configuration warns; dangerous configuration fails.** An unknown `KAHSHE_MODE`, a
`watch` mode with no rules, rules set where no watch role can run, `KAHSHE_CATALOG_IMPL` in a
serving mode — each logs a warning and continues in the safe interpretation, because a degraded
process an operator can see is better than a process that will not start. The exceptions are
`KAHSHE_WATCH_SINK` — an unknown sink name fails startup, because the failure mode there is
silence — and `KAHSHE_MAX_TOKEN_LENGTH`, which must be between 1 and `Integer.MAX_VALUE`: a cap of
0 is a plausible guess at "unlimited" and instead constructs an analyzer id no reader can parse
back, so every load would refuse the index and the term tier would prune nothing, silently.

## What this module does not do

- **No planning, pruning, index format or rule evaluation.** Those live in `proxy`, `format` and
  `watch`. If you are looking for how a plan is answered or how a bloom is read, this is the wrong
  module.
- **No configuration file and no dynamic reconfiguration.** The environment is the whole surface
  and it is read once. Changing a variable means restarting the process.
- **No durable state.** Nothing here is checkpointed. A stale index only costs performance, and the
  indexer's queue is rebuilt from observation after a restart.
- **Not a library.** Nothing depends on `app`, and nothing should; it exists to be a `main`.
- **No secret storage.** Credentials arrive as environment variables or, preferably, are never
  named at all and come from the platform's own provider chain.

## Configuration reference

Every variable, with its default and its full meaning, is in
[docs/CONFIGURATION.md](../docs/CONFIGURATION.md). `Config.fromEnv` reads about fifty of them and
`ServerTls.fromEnv` the nine TLS ones. The ones this module *owns* are the process-shaped ones —
`KAHSHE_PORT`, `KAHSHE_ADMIN_PORT`, `KAHSHE_MODE`, `KAHSHE_INDEXER`, and the TLS block — and the
rest are read here only so that one place knows the environment, then handed to the module that
owns the setting.

## Tests

```sh
./gradlew :app:check
```

`check` includes `javadoc`, which is a real gate rather than decoration: a `{@link}` left pointing
at a member a rename moved fails the build. The suites pin the wiring decisions this README
describes — that the watch roles follow the rules and not the index, so a `watch` process with the
indexer off still discovers and scans and delivers what builds elsewhere recorded; that an unset S3
key pair means the AWS default provider chain rather than a blank credential, and that an explicit
`KAHSHE_INDEX_IO_PROPERTIES` entry beats the `KAHSHE_INDEX_S3_*` shorthand for the same key; and
that a catalog kahshe has never seen loads from a class name and serves tables, while an
unresolvable one fails at startup rather than at the first poll.

One suite here is not about `app` at all: `DocTablesTest` reads the repository's shared documents —
the root, `docs/`, `benchmark/`, `dev/trino-patch/`, `helm/kahshe/`, `sigma/` and every module's
README, this one included — and fails a table row that carries more cells than its delimiter row.
That failure is otherwise silent, since GFM renders the extra cells as nothing at all.
