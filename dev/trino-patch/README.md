# Trino REST scan-planning patch (lab overlay)

Trino bundles Iceberg 1.11, which has the REST scan-planning client, but
`TrinoRestCatalog.loadTable` rewraps the loaded table into a plain `BaseTable`
to get Trino's quoted-table-name convention. That rewrap discards the
`RESTTable` scan implementation, so Trino plans locally even when the catalog
asked for server-side planning — which is exactly what kahshe's
`scan-planning-mode=server` injection asks for. This directory holds the class
overlay that fixes it for the lab image.

**Two overlaid classes, not one.** `TrinoRestCatalog.java` is the one described
above: it preserves the `RESTTable` scan, so Trino plans server-side at all.
`IcebergSplitSource.java` is the second half — it recognises the exact
token-boundary `regexp_like` pattern kahshe's `Analyzer.matchPattern` writes and
re-emits it as an equality on a `__kahshe_match__<column>` sentinel, which is how
a token predicate reaches the term index; Trino cannot push a substring or
regexp predicate into an Iceberg expression on its own. `./gradlew check`
type-checks and tests both against the real Trino 483 artifacts. The ClickBench
regexp result needs both classes: `TrinoRestCatalog` alone gives server-side
planning with nothing pushed for it to prune on.

The upstream-comparison and measurement sections below are about
`TrinoRestCatalog` only, and say so; PR #30891 is that class's change alone.

## Status: upstream, and how this copy differs

The fix is proposed upstream as **[trinodb/trino#30891][pr]**, open since
2026-08-26 from `JohnEarle:preserve-rest-table-scan-planning`.

**The upstream change and the file in this directory are no longer the same
patch**, and the difference matters if you are reading one to understand the
other:

| | `TrinoRestCatalog.java` here | PR #30891 |
|---|---|---|
| trigger | `baseTable instanceof SupportsDistributedScanPlanning` | `!baseTable.allowDistributedPlanning()` |
| delegating class | private inner `ServerPlannedTable` | top-level `ServerPlannedTable.java` |
| tests | none | `TestTrinoRestCatalog` + `ScanPlanningRestCatalogAdapter` |
| size | +37 / −5, one file | +5 in `TrinoRestCatalog`, four other files |

The copy here is deliberately the older variant: it is the one the lab actually
measured (the lab record names it "the +37-line PR #30891 change"), and a single
self-contained class is what the jar-overlay recipe below can drop in. Do not
sync it to the PR without re-running the lab measurement — the two triggers are
not the same predicate, and the numbers in the lab record belong to this one.

If you are picking up the upstream side, work in the Trino checkout, not here.
The pre-submission drafts that once sat here (`PR-DESCRIPTION.md`,
`PR-CHECKLIST.md`) were deleted once #30891 was opened. One item from them still
stands: the description's body was pasted verbatim into the PR and still offers
to add an integration test, which the pushed commit has since made untrue. That
is a PR body to update on GitHub, not a file to edit here.

## Measured

Against Trino 483, stock docker image, one class replaced: Trino delegates
`planTableScan` to kahshe; a range query on the local 50-file corpus had 50
files considered and 1 returned server-side, results correct. `LIKE '%...%'`
results are correct but unpruned — Trino cannot push substring predicates into
Iceberg expressions, so the plan request arrives unfiltered and the index never
gets asked. That is the `contains` extension / connector `match_tokens` roadmap
item, not a defect in this patch.

**Provenance, because it is not clean.** That 50→1 result was recorded at commit
`cf116a3`, when the patch was a bare `return baseTable` early return. Commit
`1bd8e61` replaced that with the `ServerPlannedTable` wrapper now in the file and
did not touch this README, so the claim has never been re-run against the code it
now sits beside. The wrapper *was* measured, separately, at the lab (id-range
4.9s stock → 1.8s server-planned, 10k-file corpus). Both results point the same
way and neither is in doubt, but if you need a single verified number for the
current file, that lab measurement is the one to quote.

## Build

The patched jar is gitignored (`io.trino_trino-iceberg-*.jar`), so regenerate it
rather than looking for it. Both `.java` files in this directory are already
patched — compile those, not a fresh download:

```
docker run --rm -v "$PWD":/patch --entrypoint sh trinodb/trino:483 -c '
  export PATH=/usr/lib/jvm/jdk-25.0.3+9/bin:$PATH
  mkdir -p /tmp/out
  javac -nowarn -cp "/usr/lib/trino/plugin/iceberg/*:/usr/lib/trino/lib/*" -d /tmp/out \
    /patch/TrinoRestCatalog.java /patch/IcebergSplitSource.java
  cp /usr/lib/trino/plugin/iceberg/io.trino_trino-iceberg-483.jar /tmp/patched.jar
  cd /tmp/out && jar uf /tmp/patched.jar \
    io/trino/plugin/iceberg/catalog/rest/TrinoRestCatalog*.class \
    io/trino/plugin/iceberg/IcebergSplitSource*.class
  cp /tmp/patched.jar /patch/io.trino_trino-iceberg-483.jar'
```

Both `*.class` globs are load-bearing: they pick up the inner classes —
`ServerPlannedTable` under `TrinoRestCatalog`, `FileScanTaskWithContext` and
`PartitionConstraintMatcher` under `IcebergSplitSource` — alongside the outer
ones. Compiling only `TrinoRestCatalog.java` leaves the stock
`IcebergSplitSource` in the jar and no token predicate is ever pushed, which
looks like an index that does not prune rather than like a missing patch.

To retarget a different Trino version, start from that version's stock source
and re-apply the change:

```
curl -sf https://raw.githubusercontent.com/trinodb/trino/<version>/plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/catalog/rest/TrinoRestCatalog.java -o TrinoRestCatalog.java
curl -sf https://raw.githubusercontent.com/trinodb/trino/<version>/plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergSplitSource.java -o IcebergSplitSource.java
# then re-apply both — `git log -p -- dev/trino-patch/ trino-patch/` shows them; both
# pathspecs, because the directory moved under dev/ at 00bf78c
```

Both files have to be retargeted together: a stock `IcebergSplitSource` beside a
patched `TrinoRestCatalog` compiles and runs, and silently prunes nothing.

## Run

Mount the patched jar over the plugin jar and point a REST catalog at kahshe;
`catalog/lake.properties` is a working example (lab credentials, not secrets).
Polaris needs `iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL`, and
`fs.hadoop.enabled=true` is required for `file://` lab warehouses.

[pr]: https://github.com/trinodb/trino/pull/30891
