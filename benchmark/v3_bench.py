"""v3 extras: exact counts and plan-latency passes.

Companion to run_bench.py — same auth, REPS/WARMUP, and summary idioms.

Subcommands:
- count:  kahshe _count (exact-or-refuse) vs OpenSearch match count
- plans:  plan-latency-only pass under a --label (for cache experiments)

Each subcommand merges its section into results-v3-extras.json
(read-modify-write; run_bench's results.json is untouched).

The `search` subcommand is gone with the relevance-search tier it drove: kahshe
answers plans and counts, never ranked rows, so there is no `_search` endpoint
left to time. The `search` section already in results-v3-extras.json is a
historical record of a capability the binary no longer has.
"""

import argparse
import json
import os
import statistics
import time
import requests

import bench_paths

OS_URL = "http://localhost:9200"
KAHSHE = "http://localhost:8282"
COUNT_PATH = bench_paths.COUNT_PATH
PLAN_PATH = bench_paths.PLAN_PATH
HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS_PATH = os.path.join(HERE, "results-v3-extras.json")
REPS = 35
WARMUP = 5

token = requests.post(
    f"{KAHSHE}/v1/oauth/tokens",
    data={
        "grant_type": "client_credentials",
        "client_id": "root",
        "client_secret": "s3cr3t",
        "scope": "PRINCIPAL_ROLE:ALL",
    },
).json()["access_token"]
AUTH = {"Authorization": f"Bearer {token}"}


def summarize(samples):
    kept = samples[WARMUP:]
    kept_sorted = sorted(kept)
    return {
        "p50_ms": round(statistics.median(kept), 2),
        "p90_ms": round(kept_sorted[int(len(kept) * 0.9) - 1], 2),
        "min_ms": round(kept_sorted[0], 2),
        "max_ms": round(kept_sorted[-1], 2),
        "n": len(kept),
    }


def save_section(key, value, label=None):
    data = json.load(open(RESULTS_PATH)) if os.path.exists(RESULTS_PATH) else {}
    if label is None:
        data[key] = value
    else:
        data.setdefault(key, {})[label] = value
    with open(RESULTS_PATH, "w") as f:
        json.dump(data, f, indent=2)
    where = f"{key}[{label}]" if label else key
    print(f"merged '{where}' into {os.path.basename(RESULTS_PATH)}")


def contains(term):
    return {"type": "contains", "term": "msg", "value": term}


def kahshe_count(term):
    t0 = time.perf_counter()
    r = requests.post(
        f"{KAHSHE}{COUNT_PATH}",
        json={"column": "msg", "term": term},
        headers=AUTH,
    )
    wall_ms = (time.perf_counter() - t0) * 1000
    return wall_ms, r.status_code, r.json()


def kahshe_plan(plan_filter):
    t0 = time.perf_counter()
    r = requests.post(f"{KAHSHE}{PLAN_PATH}", json={"filter": plan_filter}, headers=AUTH)
    plan_ms = (time.perf_counter() - t0) * 1000
    r.raise_for_status()
    return plan_ms, len(r.json().get("file-scan-tasks", []))


def os_match(query, size):
    t0 = time.perf_counter()
    r = requests.post(
        f"{OS_URL}/{bench_paths.OS_INDEX}/_search?request_cache=false",
        json={"size": size, "track_total_hits": True, "query": {"match": {"msg": query}}},
    )
    wall_ms = (time.perf_counter() - t0) * 1000
    r.raise_for_status()
    d = r.json()
    return wall_ms, d["took"], d["hits"]["total"]["value"]


COUNT_TERMS = ["connection", "quorum", "zzzabsent"]

PLAN_FILTERS = [
    ("needle_1file", contains("quorum-epoch-777")),
    ("common_noprune", contains("connection")),
    (
        "composed_range_needle",
        {
            "type": "and",
            "left": {"type": "gt-eq", "term": "id", "value": 1_150_000},
            "right": contains("cell-drain-alpha"),
        },
    ),
]


def cmd_count(_args):
    section = {"timestamp": time.time(), "terms": {}}
    for term in COUNT_TERMS:
        walls, status, data = [], None, None
        for _ in range(REPS):
            w, status, data = kahshe_count(term)
            walls.append(w)
        k_count = data.get("count") if status == 200 else None
        kahshe_entry = {**summarize(walls), "status": status, "count": k_count}
        if status != 200:
            kahshe_entry["error"] = data.get("error", {}).get("message")

        os_walls, tooks, total = [], [], None
        for _ in range(REPS):
            w, t, total = os_match(term, size=0)
            os_walls.append(w)
            tooks.append(t)
        os_entry = {
            **summarize(os_walls),
            "took_p50_ms": statistics.median(tooks[WARMUP:]),
            "count": total,
        }

        equal = status == 200 and k_count == total
        section["terms"][term] = {"kahshe": kahshe_entry, "os": os_entry, "counts_equal": equal}
        print(
            f"count '{term}': kahshe p50={kahshe_entry['p50_ms']}ms status={status} "
            f"count={k_count} | os p50={os_entry['p50_ms']}ms count={total} | equal={equal}"
        )
    save_section("count", section)


def cmd_plans(args):
    entry = {"timestamp": time.time(), "queries": {}}
    for name, plan_filter in PLAN_FILTERS:
        times, files = [], None
        for _ in range(REPS):
            plan_ms, files = kahshe_plan(plan_filter)
            times.append(plan_ms)
        s = {**summarize(times), "files_kept": files}
        entry["queries"][name] = s
        print(
            f"plans[{args.label}] {name}: p50={s['p50_ms']}ms p90={s['p90_ms']}ms "
            f"min={s['min_ms']}ms files={files}"
        )
    save_section("plans", entry, label=args.label)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("count", help="kahshe _count vs OS match count")
    plans = sub.add_parser("plans", help="plan-latency-only pass for cache experiments")
    plans.add_argument("--label", required=True, help="key to file this pass under")
    args = parser.parse_args()
    {"count": cmd_count, "plans": cmd_plans}[args.cmd](args)


if __name__ == "__main__":
    main()
