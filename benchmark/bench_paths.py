"""The names and artifact paths every script in this directory shares.

Three things here have one spelling and used to have six: the Iceberg table the corpus is
seeded into, the OpenSearch index it is mirrored to, and the bloom tier's directory under the
table's index root. Each was a string literal in whichever script needed it, which is how
00bf78c — a rename of the `bench/` directory to `benchmark/` — swept the drivers' copies of the
table and index names to `benchmark` and left the seeders making `bench`. A driver that names
an object no producer creates cannot fail informatively: it gets a 404 from a catalog that is
working correctly.

The bloom directory is the same failure in the other direction. It is keyed by Iceberg field id
(`IndexMeta.dir` -> `ngram-bloom-f<fieldId>`), for the reason IndexMeta's javadoc gives: a column
name is not a stable identity, so a name-keyed directory would hand a re-added column the old
column's blooms. Nothing tells a caller still globbing the old name-keyed spelling that it is
reading a fossil.
"""

import glob
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))

CATALOG_PREFIX = "lakehouse"
NAMESPACE = "logs"
TABLE = "bench"
TABLE_IDENT = f"{NAMESPACE}.{TABLE}"
OS_INDEX = "bench"
COLUMN = "msg"

PLAN_PATH = f"/v1/{CATALOG_PREFIX}/namespaces/{NAMESPACE}/tables/{TABLE}/plan"
COUNT_PATH = f"/kahshe/v1/{CATALOG_PREFIX}/namespaces/{NAMESPACE}/tables/{TABLE}/_count"

# The warehouse docker-compose.yml and quickstart/ create; KAHSHE_WAREHOUSE moves it.
DEFAULT_WAREHOUSE = os.path.join(HERE, "..", "dev", "testenv", "data", "lakehouse")


def warehouse_dir():
    return os.environ.get("KAHSHE_WAREHOUSE", DEFAULT_WAREHOUSE)


def index_root(warehouse=None):
    """`IndexPaths.root`'s default: the index lives inside the table location.

    A build run with KAHSHE_INDEX_ROOT set puts it at `<root>/<table-uuid>-<location-hash>`
    instead, which this cannot reconstruct without loading the table. The benchmark stack does
    not set it; `bloom_meta_path` says so when it finds nothing, so a reader who did set it is
    not left guessing.
    """
    return os.path.join(warehouse or warehouse_dir(), NAMESPACE, TABLE, "_index")


def bloom_meta_path(column=COLUMN, warehouse=None):
    """The bloom tier metadata for one column, matched on the column the document records.

    The field id is in the directory name and the column name is not, so the directory cannot be
    constructed from the column alone — but every tier's metadata carries `properties.column`, so
    reading the candidates and selecting is both exact and independent of how many columns are
    indexed.
    """
    root = index_root(warehouse)
    pattern = os.path.join(root, "ngram-bloom-f*", "index-metadata.json")
    for candidate in sorted(glob.glob(pattern)):
        with open(candidate) as f:
            meta = json.load(f)
        # Legacy documents are flat rather than sectioned; IndexMeta still reads both.
        if meta.get("properties", meta).get("column") == column:
            return candidate

    legacy = os.path.join(root, f"ngram-bloom-{column}")
    if os.path.isdir(legacy):
        raise SystemExit(
            f"{legacy} is a name-keyed bloom tier, left by a build that predates field-id keying. "
            "Nothing has written it since, so its bytes belong to whatever snapshot was current "
            "then. Rebuild the index and delete it: measuring it would file stale numbers as "
            "current, which is worse than measuring nothing.")
    raise SystemExit(
        f"no bloom tier for column {column!r} under {pattern}. Set KAHSHE_WAREHOUSE to the "
        "warehouse this benchmark's index was built into. If that build ran with "
        "KAHSHE_INDEX_ROOT set, the tier is outside the table location entirely and this lookup "
        "cannot reach it.")
