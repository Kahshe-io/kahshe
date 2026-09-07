"""Benchmark runner (post design-review revision).

Applied methodology fixes:
- request_cache=false on every OpenSearch search + cache sanity gate on 'took'
- reference semantics per query validated against a brute-force oracle over the
  staged corpus (not just cross-system count agreement); id-set validation on
  selective queries; OS variants with different semantics (match/match_phrase)
  are reported but labeled, never gated
- 35 reps, first 5 discarded, p50/p90/min/max reported; untimed full pre-pass
  of every variant before any timing (JIT/page-cache warm across shapes)
- pyarrow CPU count capped to the Docker VM's core count (symmetry with OS)
- per-query file-level false-positive pruning stats from the manifest
- environment block (host, Docker VM, versions, topology) embedded in results
- Lucene file-extension storage breakdown for paired storage comparisons
"""

import glob
import json
import os
import re
import statistics
import subprocess
import time
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests

import bench_paths

OS_URL = "http://localhost:9200"
KAHSHE = "http://localhost:8282"
PLAN_PATH = bench_paths.PLAN_PATH
HERE = os.path.dirname(os.path.abspath(__file__))
CORPUS = os.path.join(HERE, "corpus")
REPS = 35
WARMUP = 5
ROWS_PER_BATCH = 40_000

# --- symmetry: cap pyarrow to the Docker VM's CPU count
VM_NCPU = int(
    subprocess.run(
        ["docker", "info", "--format", "{{.NCPU}}"], capture_output=True, text=True
    ).stdout.strip()
    or "4"
)
pa.set_cpu_count(VM_NCPU)

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

MANIFEST = json.load(open(os.path.join(HERE, "manifest.json")))
EXACT_MSG = (
    pq.read_table(os.path.join(CORPUS, "batch-42.parquet"), columns=["msg"])
    .column("msg")[123]
    .as_py()
)
PREFIX = "connection established peer=10.0.4"


def contains(term):
    return {"type": "contains", "term": "msg", "value": term}


def oracle_scan(spec):
    """Brute-force reference over the staged corpus with the query's exact semantics."""
    dataset = ds.dataset(sorted(glob.glob(os.path.join(CORPUS, "batch-*.parquet"))))
    return dataset.scanner(filter=pa_filter(spec), columns=["id"]).to_table().column("id").to_pylist()


def pa_filter(spec):
    kind = spec[0]
    if kind == "substr":
        return pc.match_substring(ds.field("msg"), spec[1], ignore_case=True)
    if kind == "eq":
        return ds.field("msg") == spec[1]
    if kind == "substr_and_id":
        return pc.match_substring(ds.field("msg"), spec[1], ignore_case=True) & (
            ds.field("id") >= spec[2]
        )
    if kind == "prefix":
        return pc.starts_with(ds.field("msg"), pattern=spec[1])
    if kind == "id_range":
        return (ds.field("id") >= spec[1]) & (ds.field("id") < spec[2])
    raise ValueError(kind)


def wc(term):
    return {"wildcard": {"msg.wc": {"value": f"*{term}*", "case_insensitive": True}}}


QUERIES = [
    {
        "name": "needle_1file",
        "desc": "substring, 35 rows in 2 files (incl. mixed-case confounder)",
        "term": "quorum-epoch-777",
        "plan_filter": contains("quorum-epoch-777"),
        "pa": ("substr", "quorum-epoch-777"),
        "os": {"wildcard": (wc("quorum-epoch-777"), "substring (gated)")},
        "os_labeled": {
            "phrase": ({"match_phrase": {"msg": "quorum epoch 777"}}, "token-phrase, different semantics")
        },
        "validate_ids": True,
    },
    {
        "name": "needle_3files",
        "desc": "substring, 120 rows in 3 files",
        "term": "cell-drain-alpha",
        "plan_filter": contains("cell-drain-alpha"),
        "pa": ("substr", "cell-drain-alpha"),
        "os": {"wildcard": (wc("cell-drain-alpha"), "substring (gated)")},
        "os_labeled": {
            "phrase": ({"match_phrase": {"msg": "cell drain alpha"}}, "token-phrase, different semantics")
        },
        "validate_ids": True,
    },
    {
        "name": "scattered_50files",
        "desc": "substring, 100 rows spread over ALL 50 files (defeats file pruning)",
        "term": "lease-fence-omega",
        "plan_filter": contains("lease-fence-omega"),
        "pa": ("substr", "lease-fence-omega"),
        "os": {"wildcard": (wc("lease-fence-omega"), "substring (gated)")},
        "validate_ids": True,
    },
    {
        "name": "absent",
        "desc": "term that exists nowhere",
        "term": "zebra-quantum-zzz",
        "plan_filter": contains("zebra-quantum-zzz"),
        "pa": ("substr", "zebra-quantum-zzz"),
        "os": {"wildcard": (wc("zebra-quantum-zzz"), "substring (gated)")},
    },
    {
        "name": "mid_10files",
        "desc": "substring, 10.5k rows in 10 files (incl. 'checkpoint-stalled' superstring)",
        "term": "checkpoint-stall",
        "plan_filter": contains("checkpoint-stall"),
        "pa": ("substr", "checkpoint-stall"),
        "os": {"wildcard": (wc("checkpoint-stall"), "substring (gated)")},
        "os_labeled": {
            "phrase": ({"match_phrase": {"msg": "checkpoint stall"}}, "token-phrase, misses superstring")
        },
    },
    {
        "name": "common_noprune",
        "desc": "substring in ~20% of rows, all files (kahshe full-scan worst case)",
        "term": "connection",
        "plan_filter": contains("connection"),
        "pa": ("substr", "connection"),
        "os": {"wildcard": (wc("connection"), "substring (gated)")},
        "os_labeled": {
            "match": ({"match": {"msg": "connection"}}, "token, misses 'reconnection'")
        },
    },
    {
        "name": "composed_range_needle",
        "desc": "id >= 1.15M AND cell-drain-alpha (standard + extension)",
        "term": "cell-drain-alpha",
        "plan_filter": {
            "type": "and",
            "left": {"type": "gt-eq", "term": "id", "value": 1_150_000},
            "right": contains("cell-drain-alpha"),
        },
        "pa": ("substr_and_id", "cell-drain-alpha", 1_150_000),
        "os": {
            "bool_wildcard": (
                {
                    "bool": {
                        "filter": [
                            {"range": {"id": {"gte": 1_150_000}}},
                            wc("cell-drain-alpha"),
                        ]
                    }
                },
                "substring+range (gated)",
            )
        },
        "validate_ids": True,
    },
    {
        "name": "exact_eq",
        "desc": "full-message equality (standard protocol both sides)",
        "plan_filter": {"type": "eq", "term": "msg", "value": EXACT_MSG},
        "pa": ("eq", EXACT_MSG),
        "os": {"term_raw": ({"term": {"msg.raw": {"value": EXACT_MSG}}}, "exact (gated)")},
        "validate_ids": True,
    },
    {
        "name": "prefix",
        "desc": "message prefix (standard starts-with; grams common -> no pruning expected)",
        "plan_filter": {"type": "starts-with", "term": "msg", "value": PREFIX},
        "pa": ("prefix", PREFIX),
        "os": {"prefix_raw": ({"prefix": {"msg.raw": {"value": PREFIX}}}, "prefix (gated)")},
    },
    {
        "name": "id_range_only",
        "desc": "pure numeric range, no text (Iceberg stats strength)",
        "plan_filter": {
            "type": "and",
            "left": {"type": "gt-eq", "term": "id", "value": 1_000_000},
            "right": {"type": "lt", "term": "id", "value": 1_040_000},
        },
        "pa": ("id_range", 1_000_000, 1_040_000),
        "os": {
            "range": (
                {"range": {"id": {"gte": 1_000_000, "lt": 1_040_000}}},
                "range (gated)",
            )
        },
    },
]


def os_search(body, timed=True):
    t0 = time.perf_counter()
    r = requests.post(
        f"{OS_URL}/{bench_paths.OS_INDEX}/_search?request_cache=false",
        json={"size": 0, "track_total_hits": True, "query": body},
    )
    wall_ms = (time.perf_counter() - t0) * 1000
    r.raise_for_status()
    d = r.json()
    return wall_ms, d["took"], d["hits"]["total"]["value"]


def os_ids(body):
    r = requests.post(
        f"{OS_URL}/{bench_paths.OS_INDEX}/_search?request_cache=false",
        json={
            "size": 10000,
            "_source": False,
            "docvalue_fields": ["id"],
            "query": body,
        },
    )
    r.raise_for_status()
    return sorted(h["fields"]["id"][0] for h in r.json()["hits"]["hits"])


def kahshe_plan(plan_filter):
    t0 = time.perf_counter()
    r = requests.post(
        f"{KAHSHE}{PLAN_PATH}",
        json={"filter": plan_filter} if plan_filter else {},
        headers=AUTH,
    )
    plan_ms = (time.perf_counter() - t0) * 1000
    r.raise_for_status()
    tasks = r.json().get("file-scan-tasks", [])
    paths = [t["data-file"]["file-path"].removeprefix("file://") for t in tasks]
    return plan_ms, paths


def pa_rows(paths, spec, ids=False):
    if not paths:
        return [] if ids else 0
    dataset = ds.dataset(paths, format="parquet")
    scanner = dataset.scanner(filter=pa_filter(spec), columns=["id"])
    if ids:
        return sorted(scanner.to_table().column("id").to_pylist())
    return scanner.count_rows()


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


def cache_suspect(tooks):
    kept = tooks[WARMUP:]
    if all(t == 0 for t in kept):
        return True
    head = statistics.median(tooks[:3]) or 1
    tail = statistics.median(kept[-5:])
    return tail * 10 < head and head > 5


def file_batch_map(paths):
    """file path -> batch number, via each file's min id (footer-only read)."""
    result = {}
    for p in paths:
        min_id = pq.ParquetFile(p).metadata.row_group(0).column(0).statistics.min
        result[p] = min_id // ROWS_PER_BATCH
    return result


def environment():
    host_cpu = subprocess.run(["sysctl", "-n", "hw.ncpu"], capture_output=True, text=True).stdout.strip()
    host_mem = subprocess.run(["sysctl", "-n", "hw.memsize"], capture_output=True, text=True).stdout.strip()
    chip = subprocess.run(
        ["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True
    ).stdout.strip()
    vm_mem = subprocess.run(
        ["docker", "info", "--format", "{{.MemTotal}}"], capture_output=True, text=True
    ).stdout.strip()
    os_version = requests.get(OS_URL).json()["version"]["number"]
    return {
        "host": {"chip": chip, "ncpu": int(host_cpu), "mem_bytes": int(host_mem)},
        "docker_vm": {"ncpu": VM_NCPU, "mem_bytes": int(vm_mem)},
        "pyarrow_cpu_cap": VM_NCPU,
        "versions": {
            "opensearch": os_version,
            "iceberg": "1.11.0",
            "pyarrow": pa.__version__,
        },
        "topology": (
            "OpenSearch in Docker Desktop Linux VM, data on named volume (VM-native fs). "
            "Polaris in same VM. kahshe JVM and pyarrow scan run natively on the macOS host "
            "against host-local parquet; pyarrow capped to VM core count. Residual asymmetry: "
            "OS is CPU-limited to the VM; kahshe scan I/O path is host APFS. Corpus fits in "
            "page cache on both sides: this is a warm, CPU-bound comparison."
        ),
    }


def lucene_breakdown():
    # Scoped to this index's own uuid, not indices/*: the node's data directory also holds its
    # top_queries bookkeeping and, during a storage-tuned run, the two ~300MB variants os_tuned.py
    # builds. The wildcard silently adds them to a number this benchmark reports as one index's.
    uuid = requests.get(f"{OS_URL}/{bench_paths.OS_INDEX}/_settings").json()[
        bench_paths.OS_INDEX]["settings"]["index"]["uuid"]
    out = subprocess.run(
        ["docker", "exec", "kahshe-bench-os", "sh", "-c",
         f"ls -l /usr/share/opensearch/data/nodes/0/indices/{uuid}/0/index/"],
        capture_output=True, text=True,
    ).stdout
    groups = {
        "stored_fields(_source)": ("fdt", "fdx", "fdm"),
        "inverted_index": ("tim", "tip", "tmd", "doc", "pos", "pay"),
        "doc_values": ("dvd", "dvm"),
        "points": ("kdd", "kdi", "kdm"),
        "norms": ("nvd", "nvm"),
    }
    sizes = {k: 0 for k in groups}
    sizes["other"] = 0
    for line in out.splitlines():
        m = re.match(r"[-rw]+ +\d+ +\S+ +\S+ +(\d+) .* (\S+)$", line)
        if not m:
            continue
        size, name = int(m.group(1)), m.group(2)
        ext = name.rsplit(".", 1)[-1] if "." in name else name
        for group, exts in groups.items():
            if ext in exts:
                sizes[group] += size
                break
        else:
            sizes["other"] += size
    return sizes


def main():
    results = {"env": environment(), "queries": {}, "validation": {}}

    _, all_paths = kahshe_plan(None)
    batch_of = file_batch_map(all_paths)
    parquet_bytes = sum(os.path.getsize(p) for p in all_paths)

    meta_path = bench_paths.bloom_meta_path()
    index_meta = json.load(open(meta_path))
    snap = index_meta.get("snapshots", [index_meta])[0]
    props = index_meta.get("properties", index_meta)
    leaves = snap.get("leaf-files") or [index_meta.get("leafFile")]
    os_build = json.load(open(os.path.join(HERE, "os_build.json")))
    results["storage"] = {
        "parquet_bytes": parquet_bytes,
        "bloom_index_bytes_raw": snap.get("index-bytes", index_meta.get("indexBytes")),
        "bloom_leaf_on_disk_bytes": sum(
            os.path.getsize(p.removeprefix("file://")) for p in leaves),
        "bloom_config": {"ngram": int(props.get("ngram", 0)), "target_fpp": float(props.get("fpp", 0))},
        "opensearch_store_bytes": os_build["store_bytes"],
        "opensearch_lucene_breakdown": lucene_breakdown(),
        "files_total": len(all_paths),
    }
    results["build"] = {
        "os_ingest_s": os_build["ingest_s"],
        "os_forcemerge_s": os_build["merge_s"],
        "os_ingest_note": "untuned single-client bulk, chunk=5000",
    }

    # untimed pre-pass: warm every shape on both systems
    for q in QUERIES:
        for body, _ in list(q["os"].values()) + list(q.get("os_labeled", {}).values()):
            os_search(body, timed=False)
        _, paths = kahshe_plan(q["plan_filter"])
        pa_rows(paths, q["pa"])

    for q in QUERIES:
        entry = {"desc": q["desc"], "os": {}, "os_labeled": {}, "kahshe": {}}
        oracle_ids = oracle_scan(q["pa"])
        oracle_count = len(oracle_ids)
        entry["oracle_count"] = oracle_count

        for variant, (body, label) in q["os"].items():
            walls, tooks, counts = [], [], []
            for _ in range(REPS):
                w, t, c = os_search(body)
                walls.append(w); tooks.append(t); counts.append(c)
            entry["os"][variant] = {
                **summarize(walls),
                "label": label,
                "took_p50_ms": statistics.median(tooks[WARMUP:]),
                "count": counts[-1],
                "matches_oracle": counts[-1] == oracle_count,
                "cache_suspect": cache_suspect(tooks),
            }

        for variant, (body, label) in q.get("os_labeled", {}).items():
            walls, tooks, counts = [], [], []
            for _ in range(REPS):
                w, t, c = os_search(body)
                walls.append(w); tooks.append(t); counts.append(c)
            entry["os_labeled"][variant] = {
                **summarize(walls),
                "label": label,
                "count": counts[-1],
                "oracle_delta": counts[-1] - oracle_count,
                "cache_suspect": cache_suspect(tooks),
            }

        plan_times, total_times, counts = [], [], []
        files_kept = None
        for _ in range(REPS):
            t0 = time.perf_counter()
            plan_ms, paths = kahshe_plan(q["plan_filter"])
            count = pa_rows(paths, q["pa"])
            total_times.append((time.perf_counter() - t0) * 1000)
            plan_times.append(plan_ms)
            counts.append(count)
            files_kept = len(paths)
        true_batches = {batch_of[p] for p in all_paths if batch_of[p] in
                        {int(b) for b in MANIFEST["terms"].get(q.get("term", ""), {}).get("per_batch", {})}}
        entry["kahshe"] = {
            **summarize(total_times),
            "plan_p50_ms": round(statistics.median(plan_times[WARMUP:]), 2),
            "files_scanned": files_kept,
            "files_total": len(all_paths),
            "files_truly_matching": len(true_batches) if q.get("term") else None,
            "count": counts[-1],
            "matches_oracle": counts[-1] == oracle_count,
        }

        if q.get("validate_ids") and oracle_count <= 10000:
            os_body = next(iter(q["os"].values()))[0]
            _, paths = kahshe_plan(q["plan_filter"])
            kahshe_id_list = pa_rows(paths, q["pa"], ids=True)
            os_id_list = os_ids(os_body)
            entry["id_sets_match"] = kahshe_id_list == os_id_list == oracle_ids
        results["queries"][q["name"]] = entry

        gate = all(d["matches_oracle"] for d in entry["os"].values()) and entry["kahshe"]["matches_oracle"]
        print(
            f"[{'OK ' if gate else 'FAIL'}] {q['name']}: oracle n={oracle_count} | kahshe "
            f"{entry['kahshe']['p50_ms']}ms ({files_kept}/{len(all_paths)} files) | "
            + " | ".join(f"os.{v} {d['p50_ms']}ms" for v, d in entry["os"].items())
        )

    with open(os.path.join(HERE, "results.json"), "w") as f:
        json.dump(results, f, indent=2)
    print("wrote results.json")


if __name__ == "__main__":
    main()
