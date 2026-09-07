# kahshe — evidence assets

Stage animations, not charts. Every scene shows the same pipeline — engine, catalog, object
storage, file field — so the arms differ only in what actually happens.

## httplogs — three arms, one predicate

    SELECT count(*) FROM logs.httplogs WHERE clientip = '71.162.18.0'   ->  2 rows
    247,249,096 rows · 991 files · Spark 3.5.3 + Iceberg 1.11.0

| file | arm | files opened | wall clock |
|---|---|---|---|
| `httplogs-stock.svg` | stock table | 991 of 991 | 6.6–11.5 s |
| `httplogs-bloom.svg` | copy + Parquet blooms | 991 of 991 | 11.3–12.4 s |
| `httplogs-kahshe.svg` | catalog swapped to kahshe | 2 of 991 | 0.34–0.44 s warm, 2.8–3.8 s cold |

**The bloom scene is the important one.** It is the only place the distinction lands
visually: blooms prune *row groups inside files*, so the file field still lights end to end
and the sweep still runs nearly the full read phase. A bar chart flattens that into "11.3 s
versus 6.6 s" and loses the reason.

**Read phases play at one-third of real time** at a rate shared across all three scenes, so
the sweeps are honestly comparable. The wall-clock bar at the bottom of each uses one 0–21 s
axis for the same reason.

Cold is drawn on the kahshe scene as a dashed ghost segment at its true position, not hidden.

## `httplogs-overhead.svg` — its own asset

Storage is a separate argument from time, so it gets a separate animation: 1,050 MB of bloom
pages against 12.1 MB of kahshe tiers. 87x more storage, to still be 26–36x slower.

## ClickBench — secondary

    regexp_like(lower(URL), '(^|[^a-z0-9])offilialog([^a-z0-9]|$)')   ->  6 rows

1,000 of 1,000 files survive Iceberg's own pruning because a regexp cannot use bounds. The
httplogs set proves kahshe beats the alternative; this proves there are predicates where the
alternative does not exist.

`clickbench-scan-arrows.svg` is the same run drawn a second way: an arrow per file instead of
a sweep, matching the kahshe scene's grammar. 1,000 arrows at 3.5 ms apart, about 17 in flight at
once, ordered **serpentine** — left to right, then right to left. Row-major order looked
broken: every 50 files the arrow jumped back to column 0, so beams appeared at both edges
simultaneously in a third of all frames and the leftmost ones read as stuck. Serpentine drops
that to zero. The original `clickbench-scan.svg` is unchanged — keep both and pick per
context. The sweep is calmer and reads better small; the arrows make the cost visceral.

Note: the arrows are hidden in the static state. All 1,000 drawn at once is a solid wedge, so
a non-animating renderer gets the lit field and the numbers instead.

## Caveats carried on the artwork

- httplogs bytes and rows are inferred from files x size; Spark's per-run input bytes were
  not captured. Times are wall clock, n=3, single node, no planning/execution split available.
- Blooms are judged against the plain copy, not stock. The plain Spark-written copy is 3.00 GB
  against the original 1.31 GB, and the bloom copy is 4.05 GB — the extra 1.05 GB is the bloom
  pages themselves, which is why the comparison is bloom-against-plain rather than bloom-against-
  stock.
- ~2 s of both Trino executions is a per-query floor, drawn as its own dim segment.
