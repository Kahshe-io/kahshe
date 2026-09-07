"""The names and artifact paths the drivers ask for, against what the producers actually make.

Every failure here is the same shape: something renamed itself and one caller did not follow.
That has happened twice. 00bf78c renamed the `bench/` directory to `benchmark/` and its path
sweep carried the Iceberg table and the OpenSearch index along with it, leaving the drivers
asking for `logs.benchmark` and `/benchmark/_search` while the seeders went on making `bench`.
Separately, the bloom tier moved from a name-keyed directory to a field-id-keyed one and the
storage step kept globbing the old spelling. Neither is detectable by reading the driver: it
looks internally consistent, and only a live stack or a real warehouse says otherwise.

So these tests read the producer and compare, rather than restating the expected string.
"""

import json
import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
BENCH = os.path.dirname(HERE)
sys.path.insert(0, BENCH)

import bench_paths  # noqa: E402  -- the scripts under test are not a package


def write_bloom_tier(warehouse, dirname, column, field_id, leaf_bytes=64_400):
    """One bloom tier on disk, shaped as IndexMeta.toJson writes it."""
    tier = os.path.join(warehouse, "logs", "bench", "_index", dirname)
    os.makedirs(tier, exist_ok=True)
    leaf = os.path.join(tier, "leaf-4640105948159743284-9199b3db.parquet")
    with open(leaf, "wb") as f:
        f.write(b"\0" * leaf_bytes)
    meta = {
        "format-version": 1,
        "uuid": "725d9828-633a-4b51-a846-0d6ed3ad58ce",
        "table-uuid": "6aa21e83-3a1e-41e8-9004-603320db6344",
        "location": "file://" + tier,
        "type": "ngram-bloom",
        "transform-function": "HASH",
        "key-column-ids": [field_id],
        "properties": {
            "column": column,
            "field-id": str(field_id),
            "ngram": "3",
            "fpp": "0.01",
        },
        "snapshots": [{
            "snapshot-id": 1,
            "source-table-snapshot-id": 4640105948159743284,
            "timestamp-ms": 1787795784987,
            "leaf-files": ["file://" + leaf],
            "files-covered": 50,
            "data-bytes": 0,
            "index-bytes": 212512,
        }],
    }
    with open(os.path.join(tier, "index-metadata.json"), "w") as f:
        json.dump(meta, f, indent=2)
    return tier


class BloomDiscovery(unittest.TestCase):
    def setUp(self):
        held = tempfile.TemporaryDirectory()
        self.addCleanup(held.cleanup)
        self.tmp = held.name

    def test_finds_the_field_id_keyed_directory(self):
        """IndexMeta.dir writes ngram-bloom-f<fieldId>. That is the only live spelling."""
        write_bloom_tier(self.tmp, "ngram-bloom-f5", "msg", 5)
        found = bench_paths.bloom_meta_path("msg", warehouse=self.tmp)
        self.assertTrue(found.endswith("/ngram-bloom-f5/index-metadata.json"), found)

    def test_selects_by_column_not_by_glob_order(self):
        """Two indexed columns share the tier root; the field id in the path is not the column."""
        write_bloom_tier(self.tmp, "ngram-bloom-f2", "host", 2)
        write_bloom_tier(self.tmp, "ngram-bloom-f5", "msg", 5)
        self.assertIn("ngram-bloom-f5", bench_paths.bloom_meta_path("msg", warehouse=self.tmp))
        self.assertIn("ngram-bloom-f2", bench_paths.bloom_meta_path("host", warehouse=self.tmp))

    def test_refuses_a_legacy_name_keyed_directory(self):
        """A leftover ngram-bloom-<column> is a pre-field-id artifact, not a stale-but-usable one.

        Silently measuring it is worse than finding nothing: the numbers land in results.json
        looking current. This repo's own dev warehouse still carries one.
        """
        write_bloom_tier(self.tmp, "ngram-bloom-msg", "msg", 5)
        with self.assertRaises(SystemExit) as caught:
            bench_paths.bloom_meta_path("msg", warehouse=self.tmp)
        self.assertIn("ngram-bloom-msg", str(caught.exception))

    def test_reports_the_pattern_it_looked_for_when_nothing_is_there(self):
        with self.assertRaises(SystemExit) as caught:
            bench_paths.bloom_meta_path("msg", warehouse=self.tmp)
        self.assertIn("ngram-bloom-f", str(caught.exception))
        self.assertIn("KAHSHE_WAREHOUSE", str(caught.exception))


class OneDefinitionOfTheSharedNames(unittest.TestCase):
    """No script but bench_paths spells the table or the index, so a sweep cannot hit half of them.

    The literals below are the two spellings this directory has used: `bench`, which the seeders
    still make, and `benchmark`, which the rename left in the drivers. Both are failures now —
    the right one in the wrong place is what the next rename will desynchronize again.
    """

    LITERALS = (
        '"logs.bench"', '"logs.benchmark"',
        "/tables/bench/", "/tables/benchmark/",
        "/bench/_search", "/benchmark/_search",
        'INDEX = "',
    )

    def test_no_script_spells_a_shared_name_itself(self):
        offenders = []
        for name in sorted(os.listdir(BENCH)):
            if not name.endswith(".py") or name == "bench_paths.py":
                continue
            with open(os.path.join(BENCH, name)) as f:
                source = f.read()
            offenders += [f"{name}: {lit}" for lit in self.LITERALS if lit in source]
        self.assertEqual([], offenders)

    def test_the_definition_is_the_one_the_stack_serves(self):
        """PlanRoutes and KahsheHandler fix the shapes; only the table name is ours to choose."""
        self.assertTrue(
            bench_paths.PLAN_PATH.endswith(f"/tables/{bench_paths.TABLE}/plan"),
            bench_paths.PLAN_PATH)
        self.assertTrue(
            bench_paths.COUNT_PATH.startswith("/kahshe/v1/"), bench_paths.COUNT_PATH)


if __name__ == "__main__":
    unittest.main()
