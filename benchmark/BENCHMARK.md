# kahshe — benchmark results

## Lab runs on public corpora (2026-09-02/03): httplogs and ClickBench

Two independent runs, kept apart — not averaged, not combined — and every figure the animated
scenes carry (the README's, and the full set under `docs/evidence/`) is also in the text here. Raw
records, SQL and runners: [evidence/](evidence/); how each scene is drawn, and what it does not:
[docs/evidence/](../docs/evidence/EVIDENCE-README.md).

### Equality on a high-cardinality column — httplogs

`SELECT count(*) FROM logs.httplogs WHERE clientip = '71.162.18.0'` — 2 rows in 247,249,096, across
991 files and 1.31 GB. Spark 3.5.3 + Iceberg 1.11.0, `local[8]`, 10 GB driver, one job at a time,
n=3.

Provenance: the committed JSONL records are all Trino runs. The four httplogs arms are Spark, driven by `spark-needle.py` in evidence/, which prints per-query results to stdout that nothing captures — so their wall-clock ranges and the three copy sizes are transcribed from the run log rather than read back from a committed record.

| arm | what it is | files read | wall clock |
|---|---|---|---|
| stock | the table as loaded | 991 of 991 | 6.6–11.5 s |
| plain | Spark-written copy, no blooms | 991 of 991 | 18.4–20.5 s |
| bloom | same copy, `write.parquet.bloom-filter-enabled.column.clientip=true`, fpp 0.01 | 991 of 991 | 11.3–12.4 s |
| **kahshe** | same Spark, catalog URI swapped to kahshe | **2 of 991** | **0.34–0.44 s** warm, 2.8–3.8 s cold |

**How to read the bloom arm.** Blooms are judged against `plain`, not `stock`: both Spark-written
copies are bigger than the table they came from — 3.00 GB and 4.05 GB against 1.31 GB — so neither
is comparable to the untouched table. Inside its own copy the bloom did real work, 18.4 s → 11.3 s,
and that it still opens all 991 files is no defect in the measurement: a Parquet bloom prunes row
groups *inside* a file, which cannot happen until the file is open. Those pages cost 1.05 GB, 35%
added to that copy; kahshe's tiers on the original cost 12.1 MB — term 11,509,403 B (0.88% of the
data, 1,149,519 terms), bloom 566,420 B (0.04%),
[drawn here](../docs/evidence/httplogs-overhead.svg) — whose term tier merges in ~70 s on 12
threads after a full read of the data, and the plan call is 17–18 ms warm, 320–540 ms cold. Warm:
**15–34× faster than stock, 26–36× faster than the bloom arm, for about 1% of the storage.**

### A predicate min/max pruning cannot touch — ClickBench

`regexp_like(lower(URL), '(^|[^a-z0-9])offilialog([^a-z0-9]|$)')` — 6 rows in 100M ClickBench hits
as shipped, unsorted on `URL`; Trino two nodes, arms alternated per round, a fresh pod per arm, the
class overlay on the kahshe arm only. Nothing prunes a regexp, so all 1,000 files survive Iceberg's
own pruning: the case with nothing to compare against rather than something slower. The term index
takes it to **4 files**, scanning **6.8 MB rather than 2,440.1 MB** and 400,000 rows rather than
99,997,497, for the same six rows — planning 24 → 30 ms, execution 8,180 → 3,000 ms on node 003
([clickbench-kahshe.svg](../docs/evidence/clickbench-kahshe.svg)). About 2 s of each execution is
Trino's per-query floor rather than scan time, so what moved is the scan portion: roughly
6,180 → 1,000 ms. Node 003 is the conservative pair on every axis, not just the ratio: it supplies
the lower stock baseline and the slower kahshe result. 001 ran 9,920 ms stock and 2,170 ms through
kahshe — a 4.6× gap on execution against 003's 2.7×. Index cost on `URL`: term 0.84%, bloom 0.33%.

### What these numbers do not say

- **The httplogs byte and row figures are inferred**, from files × size; Spark's per-run input bytes
  were not captured, so they are approximate and must not be quoted as measured. No
  planning/execution split there either — Spark local mode gives none, Trino does.
- **The gram tier's size was not recorded** for either column, and lookup bytes were not measured.
  The term and bloom figures are the whole of what was measured.
- **Confidence is low-n.** httplogs is single-node, n=3; ClickBench n=1 per node with three warm
  re-runs on 003. Lab runs on the hardware described, not an installation serving traffic.

---

## kahshe vs OpenSearch on the local corpus (2026-08-25 series, newest update first)

The updates stack newest-first over the v1 report at the bottom, each with its own raw file.

## SEARCH ARTIFACT v2 (per-file segments) — measured 2026-08-27, REMOVED 2026-08-28

> **The capability these numbers describe no longer exists.** The whole
> relevance-search tier was deleted on 2026-08-28: the `_search` endpoint, the
> per-column Lucene artifact under `lucene-v2-*-f<fieldId>/`, every
> `KAHSHE_SEARCH*` variable, the `kahshe.search` table property, the
> `kahshe index-shard` / `kahshe index-finalize` subcommands, and all 24
> `kahshe_search_*` metrics. On the lab's 48GB corpus the Lucene group objects
> were 106 of the artifact's 119 points of artifact-over-source, and across 45
> real queries the ceiling path opened zero segments of them; rebuilding
> leaf-only gave a 13x smaller artifact in a third of the time. The per-query
> records behind the published figures are in `benchmark/evidence/` as JSONL,
> alongside the exact SQL for each one.
>
> `benchmark/v2_bench.py`, the driver that produced this section, is deleted with
> the tier — all four of its subcommands drove `_search`, the `kahshe_search_*`
> build counters, or the Lucene group layout, so none of them can run against
> this binary. `benchmark/results-v2.json` is kept, unedited by hand, as the raw
> record — but see the correction below: its two sections are from different
> runs, and neither is the run tabulated here.
>
> The measurements below are left exactly as recorded: someone will propose
> this again, and they deserve the numbers. Read the forward-looking sentences
> among them ("not yet re-measured", "the crossover is above this corpus") as
> what they were at the time, not as open work.

Local corpus (2M rows, 50 files), commit b9a3226. Raw: results-v2.json.
(Note the version-name collision: the "v2 UPDATE" section further down is the
*term index* v2, a different axis — that one survives. This section is the
*search artifact* v2, which does not.)

These numbers were themselves already superseded when they were written: they
were measured before ordinals were stabilised and segments packed into group
objects.

**Correction (2026-08-29): a grouped-layout run does exist, and it is sitting in
the raw file this section points at.** `results-v2.json`'s `search` section is
timestamped 2026-08-27T12:21Z, reports `groups_opened` (a field only the grouped
layout emits), and was taken at 53 files rather than 50 — so it is a later run,
not the one tabulated below, and it overwrote whatever the table's own raw record
had been. The driver merged each subcommand's section read-modify-write, so a
re-run replaced the section in place. What that run recorded:

| query | p50 | groups opened | segments opened (of 53) |
|---|---|---|---|
| needle (`quorum-epoch-777`) | 5.90ms | 1 | 32 |
| common (`connection`) | 6.18ms | 2 | 52 |
| two tokens (`checkpoint stall`) | 2.16ms | 1 | 8 |
| three tokens (`timeout waiting quorum`) | 7.25ms | 2 | 52 |

Grouping did not rescue the per-query cost at this scale — the same conclusion
the table below reaches, reached again on the layout that replaced it. The two
sections of `results-v2.json` are from different generations: `storage`
(2026-08-27T05:34Z, artifact `lucene-v2-f5`, 32,878,334 bytes across 100 bundle
files) predates that `search` run by seven hours. Read either against the other
only with the timestamps in hand.

**The claim it was built for — incremental maintenance — holds.**

| | files read | bundles written | wall |
|---|---|---|---|
| cold full build, 50 files | 50 | 50 | 5.0s |
| append 1 file (51 total) | **1** | **1** | 1.4s |
| append 1 file out-of-band (52 total) | **1** | **1** | 2.1s |

Appending one file indexes one file, including when the commit never passes
through the proxy. Against v1, which re-read all 50 files on every commit
(3.7s locally, 238s and a ~2.6GiB peak at the lab's 10k files), maintenance is
now proportional to what arrived rather than to table size.

**Everything else at this scale is a regression, honestly stated.**

| | v1 | v2 |
|---|---|---|
| cold full build | 3.7s | 5.0s |
| artifact bytes | 29MB | 32.9MB |
| `_search` needle | 1.55ms | 3.31ms (31/50 segments, 665 bound skips) |
| `_search` common term | 1.05ms | 3.35ms (50/50, no skips possible) |
| `_search` two tokens | 1.09ms | 1.65ms (**8/50**) |
| `_search` three tokens | 2.23ms | 4.16ms (50/50) |

Composition costs per segment opened where v1 cost one index read, and 50
small segments carry more per-segment overhead than one merged index. The
score ceilings work — the two-token query opened 8 of 50 segments, and the
needle skipped 665 candidates — but at 50 files the fixed cost dominates the
savings. `ranking_exhaustive` was true everywhere (the 64-segment budget was
never reached) and no statistics clamping fired, so scores come from exact
global statistics.

**Read this as a scale trade, not a win.** v2's costs are per-query and
roughly flat in table size for selective queries; v1's cost is a whole-table
rebuild per commit. The crossover is above this corpus. The lab numbers
(10k files, where v1 needs 238s and OOMs a serving-sized pod) are what decide
whether the trade pays, and are not yet re-measured for v2.

A bug this run found and fixed (commit follows): the indexer evaluated
staleness against whatever the cached catalog returned, so an out-of-band
commit could make it decide the index was current and then record that older
snapshot as handled — wedging maintenance for that table. It now reloads when
the table it gets predates the snapshot it was notified about.

**What changed.** The v1 search artifact was one whole-snapshot Lucene index,
rebuilt from every data file on every commit — measured at 3.7s / 29MB for the
local 2M-row / 50-file corpus, and 238s / 830MB with a ~2.6GiB peak (which
OOM-killed a 2Gi pod) for the lab's 50M-row / 10k-file corpus. v2 gives each
immutable Iceberg data file its own one-segment Lucene bundle under
`lucene-v2-f<fieldId>/`, beside a `terms-<snapshotId>.parquet` leaf carrying
exact per-(term, file) statistics, with `index-metadata.json` written last as
the pointer flip. A commit indexes only the files it added; surviving files'
entries are copied forward; a snapshot is the composition of those segments.
Scoring across independently built segments uses the exact global term
statistics in the terms leaf, so BM25 does not require merging them.

**What the driver measured.** `benchmark/v2_bench.py` had four subcommands —
`incremental` (files read to absorb an append, from the `kahshe_search_*` build
counters), `search` (`_search` latency with `segments_opened` and
`ranking_exhaustive`), `storage` (Lucene artifact bytes split groups / terms
shards / metadata), and `parity` (v1 vs v2 top-k sets and scores, never run —
it needed a v1 binary on a second port). Each recorded a `proves` /
`does_not_prove` pair beside its numbers in `results-v2.json`, and
`incremental` / `parity` exited non-zero when their claim failed, so they could
gate.

Every one of those four reads something the binary no longer has, so the driver
was deleted rather than left as a script that only fails. The `incremental`
claim — that appending N files to a table of M reads N, not M — does still hold
for the surviving index tier, whose builds are incremental and whose files-read
counter is now `kahshe_index_data_files_read_total`. Nothing in this repo
measures it at present; a replacement driver would have to be written and run,
not translated from these numbers.

---

## v3 UPDATE (2026-08-26/27): byte-weighted caches + watch (+ `_search`, since removed)

Same corpus (2M rows / 50 files), same OpenSearch version (3.8.0, image now
pinned in os_setup.sh), fresh side-by-side A/B of the pre-feature binary
(commit 3c5d1a9) and v3 (commit e8d0572) in one session. Raw: results.json
(v3 run), results-v3-extras.json.

The cache and watch results below stand. The `_search` block does not: that
endpoint was removed on 2026-08-28 (see the section above). Its numbers are
left in place as a record, and `v3_bench.py`'s `search` subcommand went with the
endpoint. `count` and `plans` are unaffected and still run.

**Correction (2026-08-29), two of them.** First, the `search` section in
`results-v3-extras.json` was *not* removed — it is still there, and
`v3_bench.py`'s docstring says so explicitly ("a historical record of a
capability the binary no longer has"). Keeping it is the right call and matches
how `results-v2.json` is treated; this paragraph was simply wrong about the file.

Second, and more useful to anyone reproducing: **the `_search` table below is
not backed by the raw file this section cites.** That section of
`results-v3-extras.json` now holds a later run (2026-08-27T13:26Z, 53 files,
grouped layout, kahshe p50 6.75 / 6.01 / 1.96 / 6.52 against OS 6.13 / 3.24 /
2.89 / 4.91) which overwrote it. The four numbers below survive in two places:
this document, and the `recorded_v1_baseline` block of `results-v2.json` — which
names *this document* as their source. So for the `_search` row, BENCHMARK.md is
the primary record and there is no JSON behind it. `_count` is unaffected: those
three numbers match `results-v3-extras.json`'s `count` section exactly.

**Plan-path regression check — none.** Every query's p50 within ±2% of the
old binary (needle 5.42ms vs 5.59; scattered 34.39 vs 34.42; absent 1.06 vs
1.14; id-range 1.18 vs 1.30). The cache engine swap (entry-count LRU →
byte-budgeted Caffeine, `KAHSHE_CACHE_BYTES`) costs nothing on the hot path;
what it buys is a bounded worst case instead of the old unbounded one. All
oracle gates green on both binaries.

**`_search` (new then, gone now): ranked BM25 references, faster than
OpenSearch here.** Removed 2026-08-28 — kahshe returns plans and counts, never
ranked rows. Historical record only.

| query | kahshe `_search` p50 | OpenSearch match p50 (took) | top hit verified |
|---|---|---|---|
| quorum-epoch-777 | **1.55ms** | 4.08ms (2.0) | yes |
| connection | **1.05ms** | 2.03ms (0) | yes |
| checkpoint stall | **1.09ms** | 2.31ms (0) | yes |
| timeout waiting quorum | **2.23ms** | 3.79ms (2.0) | yes |

OS `took` at or under its 1ms resolution — read this as "same class, kahshe
at the front", not an order-of-magnitude claim; both sides pay HTTP. The
Lucene artifact: 2M docs, built in 3.7s, 29MB on disk — vs the OS store at
179–309MB and 37.1s ingest + 4.4s forcemerge. Responses are references
(file, row_pos, score) + a coverage block; engines fetch and re-verify rows.

**`_count` (v2 endpoint, now benchmarked):** exact counts, all equal to OS,
faster: connection 0.96ms vs 1.63ms (319,731 = 319,731), quorum 0.76 vs
1.59, absent 0.73 vs 1.55.

**Watch overhead: zero at this scale.** Full index build with 10 active
rules firing 315 alerts: median 3.26s vs 3.25s bare (three runs each).

The cache-eviction path is covered by unit tests, not this benchmark: with
one table the 8 MiB per-cache floors mean the budget can never be exceeded —
by design. Multi-table eviction behavior shows up in the weight/eviction
gauges (`kahshe_*_cache_weight_bytes`, `_evictions_total`).

---

## v2 UPDATE (same day): after caching + term index

Snapshot-keyed plan caching (SnapshotPlan + CachingCatalog + commit-observed
invalidation) and the v2 term index collapsed the plan floor from ~42ms to
0.9–2.1ms. Full suite re-run, all oracle gates and id-set validations passing:

| query | v1 p50 | **v2 p50** | OpenSearch | v2 verdict |
|---|---|---|---|---|
| needle, 2 files | 47.5ms | **5.2ms** | 2.65ms | 2.0× — same order |
| needle, 3 files | 46.2ms | **5.7ms** | 2.63ms | 2.2× — same order |
| absent term | 42.1ms | **0.98ms** | 2.45ms | **kahshe wins** |
| mid, 10 files | 51.9ms | **10.5ms** | 2.33ms | 4.5× |
| range ∧ needle | 46.4ms | **4.7ms** | 2.58ms | 1.8× |
| id range only | 41.9ms | **1.19ms** | 1.60ms | **kahshe wins** |
| scattered (50/50 scan) | 76.6ms | 34.5ms | 2.43ms | full scan, structural |
| common (50/50 scan) | 78.6ms | 34.5ms | 2.25ms | full scan — but `_count` answers it in 8.8ms exactly (319,731 == OS match count), ~8ms of which is an uncached per-request auth check |
| exact eq (50/50 scan) | 74.9ms | 33.3ms | 1.66ms | needs SCALAR key-lookup index |
| prefix (50/50 scan) | 76.2ms | 32.8ms | 1.74ms | grams common; structural |

v2 additions: pinned `kahshe-ascii-v1` analyzer, aggregate-first term index
(term → file bitmap + df/cf), `match` extension (token pruning, server-side
tokenization), norms captured for future BM25, `_count` endpoint
(exact-or-refusing, coverage block, caller-token auth). Term sidecars as
measured then: aggregate 310KB + norms 788KB + postings 8.0MB (postings unit to
be tuned). Format was adversarially reviewed before implementation (9 blockers
addressed; see the project documentation). Caching gotcha for reproducers: cached plans must
be built with `includeColumnStats()` or min/max pruning silently dies.

**Correction (2026-08-28): two of those three sidecars are gone.** The postings
and norms leaves were written on every build and read by nothing — the plan-only
path never touched them, and the BM25 they were groundwork for left with the
`_search` endpoint. `TermIndexWriter` no longer emits `postings-*.parquet` or
`norms-*.parquet`, and the term metadata advertises only `aggregate` (plus
`grams` when the gram layer is on). At this corpus that is 8.8MB of the 9.1MB
of term sidecars; on the 10k-file lab corpus it was 296MB of unread leaves
against a 59MB useful index. Read the byte split above as a record of what the
build used to write, not as current sizing.

---

## v1 report (pre-caching, pre-term-index) follows unchanged.

2M synthetic log rows (33.1MB parquet, 50 Iceberg files) vs OpenSearch 3.8.0
(single node, 1 shard, forcemerged, wildcard field type for substring, request
cache disabled). Design adversarially reviewed before running (3-lens critic
panel; 6 blockers fixed incl. shard-request-cache poisoning); results
adversarially verified after (3-lens panel; all claims confirmed with revisions,
folded in below). Raw data: results.json, os_tuned.json, manifest.json.

## Storage (the decisive win)

| | bytes | notes |
|---|---|---|
| parquet data | 33.12MB | zstd columnar |
| **bloom index (raw)** | **212.5KB** | 3-gram, 1% target FPP; 64.4KB as Parquet leaf on disk |
| Iceberg table metadata | ~4.0MB | 50 unexpired append snapshots; expire in practice |
| OS store, default mapping | 309.2MB | 9.3× kahshe data+index (8.3× incl. table metadata) |
| OS store, storage-tuned | 179.4MB | zstd_no_dict, norms off, no raw doc_values; latency unchanged → 5.4× |
| OS inverted-index structures only | 156.1MB | Lucene ext. breakdown; **735× the blooms (2.9 orders)** — bundles all fields, per-field split impossible in OS |

(_source-disabled "pure index" ablation excluded: `_recovery_source` retention
inflates the store above the with-source index — measured 330MB, invalid.)

## Latency (p50 warm, count-only queries, 30 kept samples)

| query | rows | kahshe (plan floor) | files scanned | OpenSearch |
|---|---|---|---|---|
| needle, 2 files | 35 | 47.5ms (42.2) | 2/50 | 3.2ms |
| needle, 3 files | 120 | 46.2ms (41.0) | 3/50 | 3.4ms |
| scattered, all files | 100 | 76.6ms (42.1) | 50/50 | 3.0ms |
| absent term | 0 | 42.1ms (42.1) | 0/50 | 3.3ms |
| mid, 10 files | 10,500 | 51.9ms (42.1) | 10/50 | 3.6ms |
| common (~20% rows) | 399,912 | 78.6ms (43.2) | 50/50 | 3.3ms |
| range ∧ needle | 40 | 46.4ms (40.5) | 1/50 | 3.0ms |
| exact message eq | 4 | 74.9ms (41.2) | 50/50 | 1.8ms* |
| prefix | 3,531 | 76.2ms (43.4) | 50/50 | 2.6ms* |
| id range only | 40,000 | 41.9ms (41.2) | 1/50 | 2.2ms* |

\* OS server `took`=0 (below 1ms resolution): wall time is HTTP floor; ratio is a lower bound.

**OpenSearch is 13–26× faster on substring queries, 29–41× on exact/prefix/range.
That is 1.1–1.6 orders of magnitude — not "same order".** kahshe stays in
interactive double-digit ms everywhere. The ~42ms kahshe floor decomposes
(verifier-measured) as ~30ms per-request manifest planning + ~10ms catalog
loadTable + HTTP; bloom probing is ~1.3ms for all 50 files. Manifest/table
caching in the proxy is the obvious optimization and would cut most of the floor.

## Pruning correctness and selectivity

Zero file-level false positives in every prunable case: files scanned exactly
equaled files truly containing the term (2=2, 3=3, 10=10, 0=0); the composed
query's predicate truly matches 1 file and scanned 1 (range stats ∧ bloom
jointly). Honest losses, by design: scattered term (2 rows/file) defeats
file-level pruning (50/50); common terms, exact-eq of templated messages, and
common-prefix queries share every 3-gram with every file (50/50 — a term
dictionary/hash index, i.e. the draft-spec SCALAR index, is the fix for eq).

## Semantics validation

Every gated count matched a brute-force oracle; row-level id sets identical
across oracle/kahshe/OpenSearch on all applicable queries. Labeled
different-semantics variants diverged (or matched) exactly as predicted:
match_phrase +10 (space-separated confounder), 0, −500 ('checkpoint-stalled'
superstring missed), token match −80,181 ('reconnection' missed). Counts only —
their latencies sit at the took-resolution floor.

## Build costs

OS: 37.1s ingest + 4.4s forcemerge (untuned single-client bulk). kahshe:
2.1s Iceberg write + 5.4s bloom build (console observations, not persisted in
results.json).

## What this does and does not show

Warm-cache, CPU-bound, single-node, single-client, count-only, 2M rows that fit
in page cache on both sides; OS at its best-case query topology (frozen, one
segment); Docker VM asymmetry in both directions (OS CPU-limited to the VM;
kahshe scans host APFS); corpus constructed — clustered needles are bloom's
best case, and the adversarial cases (scattered/common/superstring/mixed-case)
were planted deliberately. Capability asymmetry is fundamental: Lucene is a
row-level answer structure with ranking and aggregations; the bloom is a lossy
file-level prefilter that still needs the scan. Concurrency, freshness,
cold-cache, and document retrieval untested. Nothing here extrapolates to
production scale by itself.

## Verdict against the goals

- **"Less space": strongly confirmed.** 5.4–9.3× smaller total; index
  structures ~2.9 orders smaller; index build 7× faster than OS ingest.
- **"Rival OpenSearch" on latency: not at raw speed** (13–41× slower wall-clock)
  — but 42–79ms is interactive-class, ~30ms of it is cacheable proxy overhead,
  and every engine sharing the table gets the acceleration with zero client
  config. For needle-in-haystack over file-clustered data at lakehouse storage
  cost, the trade is real; for hot sub-5ms search UX, OpenSearch remains the tool.
- **Block format: delivered** — the index is Parquet on the table's own object
  store, laid out per the draft V4 spec shape.

Known repro debts (recorded, not fixed): hardcoded paths/creds in run_bench.py,
raw samples summarized not persisted, run order encoded only in this doc,
cache_suspect heuristic cannot distinguish sub-ms queries from cached ones.
One debt from this list is since paid: the OpenSearch image is now pinned in
`os_setup.sh` (`OS_IMAGE`, defaulting to `opensearchproject/opensearch:3.8.0`).

**`run_bench.py` does not currently run (found 2026-08-29).** Its storage step
globs for `.../_index/ngram-bloom-msg/index-metadata.json` — the *name*-keyed
bloom directory. The bloom tier is now keyed by Iceberg field ID
(`IndexMeta.dir` → `ngram-bloom-f<fieldId>`), so the glob matches nothing and
`main()` exits with "no bloom index metadata under …" before a single query is
timed. This is a one-line fix, not a rewrite, but it means the v1 numbers above
cannot be reproduced from this repo until it is made. Nothing else in the driver
was checked against the current binary, so treat the fix as the start of a
revalidation rather than the end of one.
