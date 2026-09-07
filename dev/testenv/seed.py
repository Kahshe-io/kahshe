"""Seed the lakehouse.logs.events table with several distinct data files.

Each append produces its own Parquet file with a disjoint id range and distinct
message vocabulary, so both stats-based pruning (id ranges) and future
text-index pruning (message tokens) are observable in plan results.
"""

import pyarrow as pa
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "polaris",
    type="rest",
    uri="http://localhost:8181/api/catalog",
    credential="root:s3cr3t",
    warehouse="lakehouse",
    scope="PRINCIPAL_ROLE:ALL",
)

catalog.create_namespace_if_not_exists("logs")

schema = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("level", pa.string()),
        pa.field("msg", pa.string()),
    ]
)

try:
    table = catalog.load_table("logs.events")
    print("table exists:", table.name())
except Exception:
    table = catalog.create_table("logs.events", schema=schema)
    print("created:", table.name())

BATCHES = [
    (0, "INFO", ["user login ok", "session started", "heartbeat received"]),
    (1000, "INFO", ["cache warm complete", "snapshot committed", "compaction finished"]),
    (2000, "WARN", ["disk pressure rising", "retry scheduled", "slow response upstream"]),
    (3000, "ERROR", ["connection timeout to broker", "timeout waiting for quorum", "request timeout exceeded"]),
]

for base, level, msgs in BATCHES:
    rows = {
        "id": [base + i for i in range(1000)],
        "level": [level] * 1000,
        "msg": [msgs[i % len(msgs)] + f" seq={base + i}" for i in range(1000)],
    }
    table.append(pa.table(rows, schema=schema))
    print(f"appended batch base={base} level={level}")

table.refresh()
files = list(table.scan().plan_files())
print(f"total data files: {len(files)}")
for task in files:
    print(" ", task.file.file_path.rsplit('/', 1)[-1], task.file.record_count)
