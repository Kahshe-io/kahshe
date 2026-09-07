"""Storage-tuned OpenSearch variants for the paired storage comparison.

  bench_tuned : zstd_no_dict codec, norms off on msg, no doc_values on msg.raw —
                what a storage-conscious operator ships. Latency spot-checked.
  bench_nosrc : same, plus _source disabled — approximates 'pure index structures'
                (no data copy), the closest OS analogue to bloom-index-only bytes.
"""

import glob
import json
import os
import statistics
import time
import pyarrow.parquet as pq
import requests

OS = "http://localhost:9200"
CORPUS = os.path.join(os.path.dirname(__file__), "corpus")
CHUNK = 5000


def mapping(source_enabled):
    m = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "refresh_interval": "-1",
            "index.codec": "zstd_no_dict",
        },
        "mappings": {
            "_source": {"enabled": source_enabled},
            "properties": {
                "id": {"type": "long"},
                "ts": {"type": "long"},
                "level": {"type": "keyword"},
                "service": {"type": "keyword"},
                "msg": {
                    "type": "text",
                    "norms": False,
                    "fields": {
                        "raw": {"type": "keyword", "doc_values": False},
                        "wc": {"type": "wildcard"},
                    },
                },
            },
        },
    }
    return m


def build(index, source_enabled):
    requests.delete(f"{OS}/{index}")
    requests.put(f"{OS}/{index}", json=mapping(source_enabled)).raise_for_status()
    start = time.time()
    for path in sorted(glob.glob(os.path.join(CORPUS, "batch-*.parquet"))):
        rows = pq.read_table(path).to_pylist()
        for i in range(0, len(rows), CHUNK):
            lines = []
            for row in rows[i : i + CHUNK]:
                lines.append(json.dumps({"index": {"_index": index}}))
                lines.append(json.dumps(row))
            r = requests.post(
                f"{OS}/_bulk",
                data="\n".join(lines) + "\n",
                headers={"Content-Type": "application/x-ndjson"},
            )
            r.raise_for_status()
            if r.json().get("errors"):
                raise SystemExit(f"bulk errors: {index} {path}")
    ingest_s = time.time() - start
    requests.post(f"{OS}/{index}/_refresh").raise_for_status()
    requests.post(f"{OS}/{index}/_forcemerge?max_num_segments=1", timeout=1800).raise_for_status()
    requests.post(f"{OS}/{index}/_refresh").raise_for_status()
    store = requests.get(f"{OS}/{index}/_stats/store").json()["indices"][index]["primaries"][
        "store"
    ]["size_in_bytes"]
    return {"store_bytes": store, "ingest_s": round(ingest_s, 1)}


def latency_spotcheck(index):
    out = {}
    for name, term in [("needle_1file", "quorum-epoch-777"), ("common", "connection")]:
        walls = []
        body = {
            "size": 0,
            "track_total_hits": True,
            "query": {"wildcard": {"msg.wc": {"value": f"*{term}*", "case_insensitive": True}}},
        }
        for _ in range(15):
            t0 = time.perf_counter()
            requests.post(f"{OS}/{index}/_search?request_cache=false", json=body).raise_for_status()
            walls.append((time.perf_counter() - t0) * 1000)
        out[name] = round(statistics.median(walls[3:]), 2)
    return out


def main():
    results = {}
    results["bench_tuned"] = build("bench_tuned", True)
    results["bench_tuned"]["wildcard_p50_ms"] = latency_spotcheck("bench_tuned")
    results["bench_nosrc"] = build("bench_nosrc", False)
    print(json.dumps(results, indent=2))
    with open(os.path.join(os.path.dirname(__file__), "os_tuned.json"), "w") as f:
        json.dump(results, f, indent=2)
    # keep only the primary index afterwards
    requests.delete(f"{OS}/bench_tuned")
    requests.delete(f"{OS}/bench_nosrc")


if __name__ == "__main__":
    main()
