"""§0 item 8(a): the stock-Spark needle, three arms, one falsifier.

Arms (env ARM): stock  -- Spark -> Nessie directly, the table as loaded (Parquet stats, no blooms)
                plain  -- Spark -> Nessie, a Spark-written copy WITHOUT blooms: the same encoder as
                          `bloom`, so the two differ only by the bloom (the loaded table is 1.3 GB,
                          Spark's copies 4 GB -- encoder defaults, a confound the pair removes)
                bloom  -- Spark -> Nessie, a Spark-written copy with Parquet native blooms on the id
                kahshe -- Spark -> kahshe (REST server-side planning), the original table + its index
MODE=prepare writes a copy (env BLOOM=true|false picks which); MODE=query runs the needle set and
prints RESULT lines. Iceberg's
LoggingMetricsReporter prints a ScanReport per query with result/skipped data files -- the engine's
own pruning evidence, no instrumentation of ours.
"""
import os, sys, time
from pyspark.sql import SparkSession

ARM = os.environ["ARM"]
MODE = os.environ.get("MODE", "query")
TABLE = os.environ.get("TABLE", "lab.logs.httplogs")
BLOOM_TABLE = os.environ.get("BLOOM_TABLE", "lab.logs.httplogs_bloom")
PLAIN_TABLE = os.environ.get("PLAIN_TABLE", "lab.logs.httplogs_spark")
WITH_BLOOM = os.environ.get("BLOOM", "true") == "true"
RUNS = int(os.environ.get("RUNS", "3"))
NEEDLES = os.environ.get("NEEDLES", "71.162.18.0,15.40.8.0").split(",")

spark = SparkSession.builder.appName(f"kahshe-needle-{ARM}-{MODE}").getOrCreate()


def timed(sql):
    t0 = time.perf_counter()
    rows = spark.sql(sql).collect()
    return (time.perf_counter() - t0) * 1000, rows


if MODE == "prepare":
    # 991 files of ~250k rows like the source, so file counts compare; the shuffle scatters the
    # needle's rows into random files, as they were.
    target = BLOOM_TABLE if WITH_BLOOM else PLAIN_TABLE
    props = "'write.distribution-mode'='none'"
    if WITH_BLOOM:
        props += ", 'write.parquet.bloom-filter-enabled.column.clientip'='true', 'write.parquet.bloom-filter-fpp.column.clientip'='0.01'"
    spark.sql(f"DROP TABLE IF EXISTS {target}")
    spark.sql(
        f"CREATE TABLE {target} (ts timestamp, clientip string, request string, status int, size bigint) "
        f"USING iceberg TBLPROPERTIES ({props})")
    df = spark.table(TABLE).repartition(991)
    t0 = time.perf_counter()
    df.writeTo(target).append()
    ms = (time.perf_counter() - t0) * 1000
    n = spark.sql(f"SELECT count(*) FROM {target}").collect()[0][0]
    files = spark.sql(f"SELECT count(*) FROM {target}.files").collect()[0][0]
    print(f"RESULT prepare table={target} bloom={WITH_BLOOM} rows={n} files={files} wall_ms={ms:.0f}", flush=True)
    sys.exit(0)

table = {"bloom": BLOOM_TABLE, "plain": PLAIN_TABLE}.get(ARM, TABLE)
queries = [("count", f"SELECT count(*) FROM {table}")]
for needle in NEEDLES:
    queries.append((f"eq:{needle}", f"SELECT count(*) FROM {table} WHERE clientip = '{needle}'"))
queries.append(("in", f"SELECT count(*) FROM {table} WHERE clientip IN ('{NEEDLES[0]}', '{NEEDLES[1]}')"))
for run in range(1, RUNS + 1):
    for name, sql in queries:
        ms, rows = timed(sql)
        print(f"RESULT arm={ARM} run={run} query={name} wall_ms={ms:.0f} value={rows[0][0]}", flush=True)
spark.stop()
