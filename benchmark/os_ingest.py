"""Bulk-ingest the staged corpus into OpenSearch and force-merge to one segment."""

import glob
import json
import os
import time
import pyarrow.parquet as pq
import requests

import bench_paths

OS = "http://localhost:9200"
INDEX = bench_paths.OS_INDEX
CORPUS = os.path.join(os.path.dirname(__file__), "corpus")
CHUNK = 5000

MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "refresh_interval": "-1",
    },
    "mappings": {
        "properties": {
            "id": {"type": "long"},
            "ts": {"type": "long"},
            "level": {"type": "keyword"},
            "service": {"type": "keyword"},
            "msg": {
                "type": "text",
                "fields": {
                    "raw": {"type": "keyword"},
                    "wc": {"type": "wildcard"},
                },
            },
        }
    },
}


def main():
    requests.delete(f"{OS}/{INDEX}")
    r = requests.put(f"{OS}/{INDEX}", json=MAPPING)
    r.raise_for_status()

    start = time.time()
    total = 0
    for path in sorted(glob.glob(os.path.join(CORPUS, "batch-*.parquet"))):
        table = pq.read_table(path)
        rows = table.to_pylist()
        for i in range(0, len(rows), CHUNK):
            lines = []
            for row in rows[i : i + CHUNK]:
                lines.append(json.dumps({"index": {"_index": INDEX}}))
                lines.append(json.dumps(row))
            body = "\n".join(lines) + "\n"
            resp = requests.post(
                f"{OS}/_bulk", data=body, headers={"Content-Type": "application/x-ndjson"}
            )
            resp.raise_for_status()
            if resp.json().get("errors"):
                raise SystemExit(f"bulk errors in {path}")
            total += len(rows[i : i + CHUNK])
        print(f"  {os.path.basename(path)} done ({total:,} total)")
    ingest_s = time.time() - start

    requests.post(f"{OS}/{INDEX}/_refresh").raise_for_status()
    merge_start = time.time()
    requests.post(
        f"{OS}/{INDEX}/_forcemerge?max_num_segments=1", timeout=1800
    ).raise_for_status()
    requests.post(f"{OS}/{INDEX}/_refresh").raise_for_status()
    merge_s = time.time() - merge_start

    count = requests.get(f"{OS}/{INDEX}/_count").json()["count"]
    stats = requests.get(f"{OS}/{INDEX}/_stats/store").json()
    store = stats["indices"][INDEX]["primaries"]["store"]["size_in_bytes"]
    print(f"docs: {count:,}  store: {store:,} bytes")
    print(f"ingest: {ingest_s:.1f}s  forcemerge: {merge_s:.1f}s")
    with open(os.path.join(os.path.dirname(__file__), "os_build.json"), "w") as f:
        json.dump({"docs": count, "store_bytes": store, "ingest_s": ingest_s, "merge_s": merge_s}, f)


if __name__ == "__main__":
    main()
