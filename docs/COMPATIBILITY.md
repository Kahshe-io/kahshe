# Compatibility

Which engines and catalogs get kahshe's planning benefit, verified against vendor documentation
and source at current release tags on 2026-09-06. Each row carries its confidence; "unknown" is a
verdict here, not a gap in the table.

## What decides it

An engine gets **pruned planning** only if both hold:

1. it can be pointed at a user-supplied Iceberg REST catalog URI, and
2. it plans through the REST spec's `planTableScan`. The Iceberg 1.11 Java client does when the
   table's `LoadTableResponse.config` carries `scan-planning-mode=server` and `/v1/config` lists the
   plan endpoint — kahshe supplies both. An engine that reads `metadata.json` itself and plans from
   manifests gets no benefit, and nothing breaks.

Everything else an engine sends is forwarded to the catalog unchanged.

## Engines

| engine | verdict | why | confidence |
|---|---|---|---|
| **Spark 3.4–4.1** (iceberg-spark-runtime, Iceberg 1.11) | **pruned planning**, batch reads | `SparkCatalog.load` keeps the catalog's `RESTTable`; `newBatchScan` posts to `/plan`. Structured streaming and start/end-snapshot reads use incremental scans `RESTTable` does not override, so they plan locally | verified from source; measured (httplogs) |
| **Flink 1.20–2.1** (iceberg-flink, Iceberg 1.11) | pruned planning, bounded jobs | `TableLoader` returns the catalog table unwrapped; the batch planner calls `newScan()`. Streaming on the default starting strategy never calls `/plan`. Pushes `=`, ranges, `IS NULL`, `LIKE 'x%'`; no `IN` | verified from source; not lab-run |
| **Trino 483** | passthrough stock; **pruned planning with the two-class overlay** ([dev/trino-patch/](../dev/trino-patch/README.md)) | `TrinoRestCatalog.loadTable` rewraps into `BaseTable` and discards `RESTTable`. Upstream: [trino#30891](https://github.com/trinodb/trino/pull/30891), open | verified from source at tag 483 and master |
| **PyIceberg 0.12** | pruned planning with one catalog property, `scan-planning-mode: server` (`rest-scan-planning-enabled: true` on 0.11) | `supports_server_side_planning()` reads catalog properties and the `/v1/config` endpoint list only, never the per-table config kahshe injects. PR #3724 on main gives that config precedence, so the next release is zero-config. The property is catalog-wide: a client that honours it asks kahshe to plan merge-on-read tables too and gets a 422 unless `KAHSHE_SERVE_DELETE_BEARING=true` (see Merge-on-read). **Caveat:** kahshe returns no residual filter and PyIceberg maps that to always-true, so `DataScan.count()` over-counts through kahshe; read paths re-apply the filter and are correct | verified line by line at the 0.12.0 tag |
| **Daft 0.7** (via PyIceberg ≤ 0.11.1) | same lever; `=`, `IN`, ranges, prefix only | Any other function in the filter makes Daft drop the whole filter, so substring pruning is unreachable from Daft | verified from source |
| **Polars 1.44** (`scan_iceberg`, native reader) | **do not use with kahshe yet** | kahshe strips per-file statistics by default and Polars' `IcebergStatisticsLoader` dereferences them unguarded: a filtered scan under server planning crashes. Disabling the statistics disables the filter too | derived from source, not executed |
| **Snowflake** | passthrough | Accepts the URL (`CATALOG_SOURCE = ICEBERG_REST`), then calls `getConfig`/`loadTable` and plans itself | verified from docs |
| **Starburst Enterprise** | passthrough | Proprietary Iceberg plugin; the overlay's class layout is unverifiable there | inferred |
| **Dremio 26** | passthrough | Own distributed manifest-reading operators | inferred |
| **Presto 0.299** | passthrough | Bundles Iceberg 1.10.1. It does **not** rewrap the table, so a 1.11 bump would flip it | verified from source |
| **StarRocks 4.1, Doris 4.1, ClickHouse 26.7** | passthrough | 1.10-era clients; the injected key is ignored | verified from docs/source |
| **DuckDB 1.5** (iceberg extension) | passthrough on every release | Server planning is on the extension's main branch (PR #1204, 2026-07-24) and honours kahshe's injection; no release carries it yet | verified from source |
| **Google BigQuery** | cannot sit in front | No field accepts a customer-supplied REST catalog URI; external tables take a `metadata.json` path or Glue, and federation reads manifests itself | verified from docs |
| **Amazon Athena** | cannot sit in front | Reads Iceberg only from the Glue Data Catalog, on Iceberg 1.4.2. Glue federation to a remote REST catalog is documented only for Snowflake and Databricks endpoints and still plans locally | verified from docs |
| **Databricks** (Unity Catalog reading Iceberg) | cannot sit in front | Foreign Iceberg tables via federation, own reader. Unity's own REST endpoint works as a *catalog* behind kahshe | verified from docs |
| **Microsoft Fabric** | cannot sit in front | Every read path converts Iceberg to Delta first | verified from docs |

## Catalogs

kahshe proxies any catalog that serves the Iceberg REST spec over HTTP with an auth it can forward.
Bearer tokens pass through; kahshe does not sign requests.

| catalog | verdict | the thing to know |
|---|---|---|
| **Apache Polaris 1.7** | works — the quickstart runs on it | Polaris has no `/plan` of its own |
| **Project Nessie 0.108** | works | Its prefix encodes branch and warehouse with a pipe character; kahshe round-trips it verbatim |
| **Lakekeeper 0.13** | works | No static credential: kahshe's planning identity must be an OIDC client-credentials or Kubernetes service-account token |
| **Apache Gravitino 1.3** | works, with a caveat | Gravitino answers `/plan` itself; behind kahshe, kahshe's planner replaces it |
| **Unity Catalog OSS 0.6** | works | Auth is off by default, but kahshe's served endpoints require a bearer |
| **Snowflake Open Catalog, Databricks Unity Catalog** | works | On Databricks, in `service` mode kahshe answers `/plan` with an unfiltered file list where Unity would enforce row filters — use `KAHSHE_PLANNING_IDENTITY=caller` |
| **AWS Glue Iceberg REST, S3 Tables** | **not yet** | They require SigV4 request signing, which kahshe does not do |

## Merge-on-read

A snapshot carrying delete files is refused for server planning by default. The refusal is
broader than it needs to be: measured, stock iceberg-java returns correct rows through server
planning on position and equality deletes both, and only Trino 483 breaks — it calls
`.longValue()` on a sequence number the REST scan-task format drops. `KAHSHE_SERVE_DELETE_BEARING=true`
serves Spark and Flink; leave it off while Trino reads through the proxy. The guard is per
snapshot, so a compacted table serves either way.

## Who benefits

A team whose reads go through an Iceberg 1.11+ Java client that keeps the catalog's table object —
Spark batch, Flink bounded, Trino with the overlay — against a bearer-auth REST catalog, on
append-only or compacted tables, running predicates min/max statistics cannot prune: equality or
`IN` on a high-cardinality string, prefix, or token and substring matches over log-like text. The
watch role needs none of this: it reads the table directly and serves any engine.
