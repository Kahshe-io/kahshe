"""Create the benchmark table in Polaris and append the 50 staged corpus batches (one file each)."""

import glob
import os
import time
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog

import bench_paths

CORPUS = os.path.join(os.path.dirname(__file__), "corpus")
TABLE = bench_paths.TABLE_IDENT

catalog = load_catalog(
    "polaris",
    type="rest",
    uri="http://localhost:8181/api/catalog",
    credential="root:s3cr3t",
    warehouse="lakehouse",
    scope="PRINCIPAL_ROLE:ALL",
)

catalog.create_namespace_if_not_exists(bench_paths.NAMESPACE)
try:
    catalog.drop_table(TABLE)
    print(f"dropped existing {TABLE}")
except Exception:
    pass

batches = sorted(glob.glob(os.path.join(CORPUS, "batch-*.parquet")))
first = pq.read_table(batches[0])
table = catalog.create_table(TABLE, schema=first.schema)

start = time.time()
for path in batches:
    table.append(pq.read_table(path))
elapsed = time.time() - start

table.refresh()
files = list(table.scan().plan_files())
data_bytes = sum(t.file.file_size_in_bytes for t in files)
print(f"appended {len(batches)} batches in {elapsed:.1f}s")
print(f"data files: {len(files)}  data bytes: {data_bytes:,}")
