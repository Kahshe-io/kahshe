#!/usr/bin/env python3
"""Run the ClickBench query set through a Trino and record what each query cost.

  clickbench-run.py --trino http://127.0.0.1:18080 --table iceberg.logs.clickbench \\
                    --arm trino-sp --runs 3 --out benchmark/evidence/results-clickbench.jsonl

Speaks Trino's HTTP protocol directly (POST /v1/statement, follow nextUri) rather than
`kubectl exec trino`, so per-query numbers come from the server's own query stats:
elapsed and execution time, PHYSICAL input bytes and rows, and the number of splits
the table scan ran -- which is the pruning count, and the number that matters more
than wall clock on this cluster (docs/ARCHITECTURE.md §6.5). Every record is one query in
one run in one arm, appended as JSON so runs from alternated arms interleave in the
file exactly as they did on the cluster.

The queries are benchmark/evidence/clickbench-queries.sql, ClickBench's own trino/queries.sql
with `hits` replaced by a placeholder; --queries limits a run to a subset by number.
"""
import argparse
import json
import re
import sys
import time
import urllib.request

DURATION = re.compile(r"^([0-9.]+)(ns|us|ms|s|m|h|d)$")
DATASIZE = re.compile(r"^([0-9.]+)(B|kB|MB|GB|TB|PB)$")
UNIT_MS = {"ns": 1e-6, "us": 1e-3, "ms": 1.0, "s": 1e3, "m": 60e3, "h": 3600e3, "d": 86400e3}
UNIT_B = {"B": 1, "kB": 1024, "MB": 1024 ** 2, "GB": 1024 ** 3, "TB": 1024 ** 4, "PB": 1024 ** 5}


def millis(text):
    m = DURATION.match(text or "")
    return round(float(m.group(1)) * UNIT_MS[m.group(2)], 3) if m else None


def nbytes(text):
    m = DATASIZE.match(text or "")
    return int(float(m.group(1)) * UNIT_B[m.group(2)]) if m else None


class Trino:
    def __init__(self, base, user="bench", catalog=None, schema=None):
        self.base = base.rstrip("/")
        self.headers = {"X-Trino-User": user}
        if catalog:
            self.headers["X-Trino-Catalog"] = catalog
        if schema:
            self.headers["X-Trino-Schema"] = schema

    def _json(self, url, data=None):
        req = urllib.request.Request(url, data=data, headers=self.headers,
                                     method="POST" if data is not None else "GET")
        with urllib.request.urlopen(req, timeout=3600) as r:
            return json.load(r)

    def run(self, sql):
        """Executes to completion; returns (row count, first rows, query id, error or None)."""
        page = self._json(f"{self.base}/v1/statement", sql.encode())
        qid, rows, head = page["id"], 0, []
        while True:
            for row in page.get("data", []) or []:
                rows += 1
                if len(head) < 3:
                    head.append(row)
            if "error" in page:
                return rows, head, qid, page["error"].get("message", "error")
            nxt = page.get("nextUri")
            if not nxt:
                return rows, head, qid, None
            page = self._json(nxt)

    def stats(self, qid):
        info = self._json(f"{self.base}/v1/query/{qid}")
        s = info["queryStats"]
        splits = 0
        for op in s.get("operatorSummaries", []):
            if op.get("operatorType") in ("TableScanOperator", "ScanFilterAndProjectOperator"):
                splits += op.get("totalDrivers", 0)
        return {
            "state": info.get("state"),
            "elapsed_ms": millis(s.get("elapsedTime")),
            "execution_ms": millis(s.get("executionTime")),
            "queued_ms": millis(s.get("queuedTime")),
            "planning_ms": millis(s.get("planningTime")),
            "physical_input_bytes": nbytes(s.get("physicalInputDataSize")),
            "physical_input_rows": s.get("physicalInputPositions"),
            "splits": splits,
            "peak_memory_bytes": nbytes(s.get("peakUserMemoryReservation")),
        }


def load_queries(path, table):
    """One query per line; a trailing `; -- note` is dropped (httplogs-queries.sql names Rally ops that way)."""
    out = []
    for line in open(path):
        line = line.strip()
        if line and not line.startswith("--"):
            m = re.match(r"^(.*?);\s*(--.*)?$", line)
            if m:
                line = m.group(1)
            out.append(line.replace("{table}", table))
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--trino", required=True)
    ap.add_argument("--table", required=True, help="catalog.schema.table the placeholder becomes")
    ap.add_argument("--arm", required=True, help="a label: which Trino, which index state")
    ap.add_argument("--runs", type=int, default=1)
    ap.add_argument("--queries", default="", help="comma-separated 0-based query numbers; default all")
    ap.add_argument("--sql", default="benchmark/evidence/clickbench-queries.sql")
    ap.add_argument("--out", required=True, help="JSON lines, appended")
    ap.add_argument("--extra", default="", help="path to extra queries (same placeholder), run after the set")
    args = ap.parse_args()

    queries = [(f"Q{i}", q) for i, q in enumerate(load_queries(args.sql, args.table))]
    if args.extra:
        queries += [(f"X{i}", q) for i, q in enumerate(load_queries(args.extra, args.table))]
    if args.queries:
        wanted = set(args.queries.split(","))
        queries = [(n, q) for n, q in queries if n in wanted or n[1:] in wanted]
    trino = Trino(args.trino)
    with open(args.out, "a") as out:
        for run in range(1, args.runs + 1):
            for name, sql in queries:
                t0 = time.perf_counter()
                rows, head, qid, err = trino.run(sql)
                wall = round((time.perf_counter() - t0) * 1000, 1)
                rec = {"arm": args.arm, "run": run, "query": name, "query_id": qid,
                       "wall_ms": wall, "result_rows": rows, "head": head[:2], "error": err,
                       "at": time.strftime("%Y-%m-%dT%H:%M:%S")}
                try:
                    rec.update(trino.stats(qid))
                except Exception as e:  # stats are a report, not the run
                    rec["stats_error"] = str(e)[:200]
                out.write(json.dumps(rec, default=str) + "\n")
                out.flush()
                mb = (rec.get("physical_input_bytes") or 0) / 1e6
                print(f"{args.arm:12s} run{run} {name:4s} {rec.get('elapsed_ms') or wall:>9.0f} ms  "
                      f"{rec.get('splits', '?'):>5} splits  {mb:>9.1f} MB  rows={rows}"
                      f"{'  ERROR ' + err[:60] if err else ''}", flush=True)


if __name__ == "__main__":
    main()
