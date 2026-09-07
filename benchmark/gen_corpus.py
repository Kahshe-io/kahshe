"""Deterministic synthetic log corpus: 50 batches x 40k rows staged as Parquet,
plus a brute-force oracle manifest of exact counts/ids/per-file placement.

Planted terms (see manifest.json for exact, verified counts):
  NEEDLE1   'quorum-epoch-777'  : 25 rows batch 37  (+10 mixed-case in batch 44,
                                   +10 space-separated 'quorum epoch 777' in batch 12
                                   as semantic confounders)
  NEEDLE3   'cell-drain-alpha'  : 40 rows each in batches 10, 20, 30
  SCATTERED 'lease-fence-omega' : 2 rows in EVERY batch (defeats file-level pruning)
  MID       'checkpoint-stall'  : 1,250 rows each in 8 batches (+250 rows of the
                                   superstring 'checkpoint-stalled' in batches 2 and 47)
  COMMON    'connection'        : broad via templates, including 'reconnection'
                                   (substring matches, token match does not)
  ABSENT    'zebra-quantum-zzz' : nowhere
"""

import json
import os
import random
import pyarrow as pa
import pyarrow.parquet as pq

OUT = os.path.join(os.path.dirname(__file__), "corpus")
BATCHES = 50
ROWS = 40_000
MID_BATCHES = {5, 12, 19, 26, 33, 40, 44, 48}
STALLED_BATCHES = {2: 250, 47: 250}

SERVICES = [
    "gateway", "auth", "billing", "ingest", "planner", "catalog",
    "indexer", "scheduler", "notifier", "archiver", "metering", "replicator",
]

TEMPLATES = [
    "request completed status=200 path=/api/v1/{svc}/items latency_ms={n}",
    "request completed status=404 path=/api/v1/{svc}/missing latency_ms={n}",
    "connection established peer=10.0.{o}.{o2} port=9092",
    "connection reset by peer=10.0.{o}.{o2} after {n}ms",
    "reconnection attempt {o} to peer=10.0.{o2}.{o} succeeded",
    "cache miss key={svc}:{n} fetching from origin",
    "cache hit ratio {p}% window=60s",
    "gc pause {n}ms young-gen survivors={o}",
    "compaction finished level={o} files={o2} bytes={big}",
    "snapshot committed id={big} manifests={o}",
    "lease renewed holder={svc}-{o} ttl=30s",
    "retry attempt {o} for upstream {svc} backoff={n}ms",
    "tls handshake completed cipher=TLS_AES_128_GCM_SHA256 in {n}ms",
    "connection pool exhausted waiting={o} max=64",
    "wrote {big} bytes to segment-{o2}.log in {n}ms",
    "read {big} bytes from object store prefix={svc}/{o}",
    "heartbeat ok node={svc}-{o} epoch={o2}",
    "election won term={o2} votes={o}",
    "flush completed memtable={n}kb entries={big}",
    "authentication ok user=svc-{svc} method=oauth2",
]

WARN_TEMPLATES = [
    "disk usage {p}% above soft threshold on /data/{svc}",
    "slow query {n}ms exceeds budget plan={svc}-{o}",
    "connection latency {n}ms above p99 target peer=10.0.{o}.{o2}",
    "clock skew {o}ms detected against node {svc}-{o2}",
    "certificate for {svc}.internal expires in {o2} days",
]

ERROR_TEMPLATES = [
    "request failed status=500 path=/api/v1/{svc}/commit err=internal",
    "connection refused peer=10.0.{o}.{o2} retries exhausted",
    "timeout after {n}ms waiting for {svc} quorum",
    "write conflict on key {svc}:{big} txn aborted",
    "oom killed worker pid={big} rss={n}mb",
]

# terms the oracle tracks as case-insensitive substrings
ORACLE_TERMS = [
    "quorum-epoch-777",
    "cell-drain-alpha",
    "lease-fence-omega",
    "checkpoint-stall",
    "connection",
    "zebra-quantum-zzz",
]


def render(rng, template):
    return template.format(
        svc=rng.choice(SERVICES),
        n=rng.randint(1, 5000),
        o=rng.randint(1, 250),
        o2=rng.randint(1, 250),
        p=rng.randint(1, 99),
        big=rng.randint(10_000, 99_999_999),
    )


def main():
    os.makedirs(OUT, exist_ok=True)
    rng = random.Random(42)
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("ts", pa.int64(), nullable=False),
            pa.field("level", pa.string()),
            pa.field("service", pa.string()),
            pa.field("msg", pa.string()),
        ]
    )

    manifest = {
        "batches": BATCHES,
        "rows_per_batch": ROWS,
        "terms": {t: {"count": 0, "per_batch": {}, "ids": []} for t in ORACLE_TERMS},
    }

    for b in range(BATCHES):
        ids, tss, levels, services, msgs = [], [], [], [], []
        needle1 = set(rng.sample(range(ROWS), 25)) if b == 37 else set()
        needle1_mixed = set(rng.sample(range(ROWS), 10)) if b == 44 else set()
        needle1_spaced = set(rng.sample(range(ROWS), 10)) if b == 12 else set()
        needle3 = set(rng.sample(range(ROWS), 40)) if b in (10, 20, 30) else set()
        scattered = set(rng.sample(range(ROWS), 2))
        mid = set(rng.sample(range(ROWS), 1250)) if b in MID_BATCHES else set()
        stalled = set(rng.sample(range(ROWS), STALLED_BATCHES[b])) if b in STALLED_BATCHES else set()

        for i in range(ROWS):
            row_id = b * ROWS + i
            roll = rng.random()
            if roll < 0.80:
                level, pool = "INFO", TEMPLATES
            elif roll < 0.95:
                level, pool = "WARN", WARN_TEMPLATES
            else:
                level, pool = "ERROR", ERROR_TEMPLATES
            msg = render(rng, rng.choice(pool))
            if i in needle1:
                msg += " marker=quorum-epoch-777"
            if i in needle1_mixed:
                msg += " marker=Quorum-Epoch-777"
            if i in needle1_spaced:
                msg += " note quorum epoch 777 reached"
            if i in needle3:
                msg += " marker=cell-drain-alpha"
            if i in scattered:
                msg += " marker=lease-fence-omega"
            if i in mid:
                msg += " marker=checkpoint-stall"
            if i in stalled:
                msg += " state=checkpoint-stalled"

            lower = msg.lower()
            for term in ORACLE_TERMS:
                if term in lower:
                    entry = manifest["terms"][term]
                    entry["count"] += 1
                    entry["per_batch"][str(b)] = entry["per_batch"].get(str(b), 0) + 1
                    if entry["count"] <= 20000:
                        entry["ids"].append(row_id)

            ids.append(row_id)
            tss.append(1_756_000_000_000 + row_id)
            levels.append(level)
            services.append(rng.choice(SERVICES))
            msgs.append(msg)

        table = pa.table(
            {"id": ids, "ts": tss, "level": levels, "service": services, "msg": msgs},
            schema=schema,
        )
        pq.write_table(table, os.path.join(OUT, f"batch-{b:02d}.parquet"))

    # trim huge id lists (keep counts + placement authoritative)
    for term, entry in manifest["terms"].items():
        if len(entry["ids"]) > 20000 or entry["count"] > 20000:
            entry["ids"] = None  # too many to carry; count/per_batch remain exact
    with open(os.path.join(os.path.dirname(__file__), "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"wrote {BATCHES} batches x {ROWS} rows to {OUT}")
    for term, entry in manifest["terms"].items():
        print(f"  {term}: {entry['count']:,} rows in {len(entry['per_batch'])} batches")


if __name__ == "__main__":
    main()
