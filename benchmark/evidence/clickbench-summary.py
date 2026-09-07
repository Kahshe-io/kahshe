#!/usr/bin/env python3
"""Summarise clickbench-run.py's JSON lines: per query, per arm, the median over runs.

  clickbench-summary.py benchmark/evidence/results-clickbench.jsonl [--arms trino-sp,trino]

One row per query: for each arm the median elapsed, the splits and physical input
bytes of the LAST run (they do not vary between runs unless pruning changed), and the
result-row count, with a flag when the arms disagree on rows -- which would be a
correctness finding, not a performance one.
"""
import argparse
import json
import statistics
from collections import defaultdict


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("path")
    ap.add_argument("--arms", default="trino-sp,trino")
    ap.add_argument("--markdown", action="store_true")
    args = ap.parse_args()
    arms = args.arms.split(",")
    recs = defaultdict(lambda: defaultdict(list))
    for line in open(args.path):
        r = json.loads(line)
        recs[r["query"]][r["arm"]].append(r)

    def qkey(q):
        return (q[0], int(q[1:]))

    rows = []
    for q in sorted(recs, key=qkey):
        cells = {}
        for arm in arms:
            rs = recs[q].get(arm, [])
            if not rs:
                cells[arm] = None
                continue
            el = [r.get("elapsed_ms") or r["wall_ms"] for r in rs]
            last = rs[-1]
            cells[arm] = {
                "n": len(rs), "median_ms": statistics.median(el), "min_ms": min(el), "max_ms": max(el),
                "splits": last.get("splits"), "bytes": last.get("physical_input_bytes"),
                "rows": last.get("result_rows"), "head": last.get("head"),
                "errors": sum(1 for r in rs if r.get("error")),
            }
        rows.append((q, cells))

    a, b = arms[0], arms[1] if len(arms) > 1 else None
    if args.markdown:
        print(f"| query | {a} median | {b} median | ratio | splits {a}/{b} | MB read {a}/{b} | rows |")
        print("|---|---|---|---|---|---|---|")
    else:
        print(f"{'query':6s} {a+' med':>12s} {b+' med' if b else '':>12s} {'ratio':>6s} {'splits':>12s} {'MB read':>18s} {'rows':>10s} note")
    for q, c in rows:
        ca, cb = c.get(a), c.get(b) if b else None
        ma = ca["median_ms"] if ca else None
        mb = cb["median_ms"] if cb else None
        ratio = (ma / mb) if (ma and mb) else None
        sp = f"{ca['splits'] if ca else '-'}/{cb['splits'] if cb else '-'}"
        mbs = f"{(ca['bytes'] or 0)/1e6:.0f}/{(cb['bytes'] or 0)/1e6:.0f}" if (ca and cb) else "-"
        rows_txt = f"{ca['rows'] if ca else '-'}"
        note = ""
        if ca and cb and ca["rows"] != cb["rows"]:
            note = f"ROWS DIFFER {ca['rows']} vs {cb['rows']}"
        if (ca and ca["errors"]) or (cb and cb["errors"]):
            note += f" errors {ca['errors'] if ca else 0}/{cb['errors'] if cb else 0}"
        if args.markdown:
            print(f"| {q} | {ma/1000:.1f} s | {mb/1000:.1f} s | {ratio:.2f} | {sp} | {mbs} | {rows_txt} {note} |"
                  if (ma and mb) else f"| {q} | {ma} | {mb} | | {sp} | {mbs} | {rows_txt} {note} |")
        else:
            print(f"{q:6s} {ma/1000 if ma else 0:>10.1f} s {mb/1000 if mb else 0:>10.1f} s {ratio if ratio else 0:>6.2f} {sp:>12s} {mbs:>18s} {rows_txt:>10s} {note}")
    tot = {arm: sum(c[arm]["median_ms"] for _, c in rows if c.get(arm) and _.startswith("Q")) for arm in arms}
    print("sum of per-query medians over Q0-Q42:", {k: f"{v/1000:.1f} s" for k, v in tot.items()})


if __name__ == "__main__":
    main()
