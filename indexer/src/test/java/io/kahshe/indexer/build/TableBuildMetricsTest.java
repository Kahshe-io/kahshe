package io.kahshe.indexer.build;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.Metrics;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;
import java.nio.file.Path;
import java.util.concurrent.atomic.LongAdder;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Each publish counts under its table, column and kind, and its wall time lands in the per-column
 * seconds counter — the fleet's {@code kahshe_index_builds_total} cannot say which column is
 * eating the indexer. The table label is {@code namespace.table}; the prefix, which can carry
 * tenancy, never reaches a label. Verified red with the publish site recording nothing: the
 * full-build series was absent.
 */
class TableBuildMetricsTest {
  @TempDir Path tmp;

  private static String kind(String kind) {
    return Metrics.labels("table", "logs.events", "column", LocalTableFixture.COLUMN, "kind", kind);
  }

  @Test
  void eachPublishCountsUnderItsTableColumnAndKindAndNeverThePrefix() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha", "bravo");
    BuildConfig config = LocalTableFixture.config();
    Metrics metrics = new Metrics();
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config, IndexBuildListener.NONE,
        "tenant-a", "logs", "events", metrics);
    LongAdder full = metrics.tableIndexBuilds.get(kind("full"));
    assertNotNull(full, "a column's first build is a full one; series held: " + metrics.tableIndexBuilds.keySet());
    assertEquals(1, full.sum());

    LocalTableFixture.appendFile(table, "f2.parquet", "charlie");
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config, IndexBuildListener.NONE,
        "tenant-a", "logs", "events", metrics);
    assertEquals(1, metrics.tableIndexBuilds.get(kind("incremental")).sum(), "one file appended: incremental");
    assertEquals(1, full.sum(), "and the full series did not move");

    table.refresh();
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config, IndexBuildListener.NONE,
        "tenant-a", "logs", "events", metrics);
    assertEquals(1, metrics.tableIndexBuilds.get(kind("restamp")).sum(), "nothing new: restamp");

    LongAdder millis = metrics.tableIndexBuildMillis.get(
        Metrics.labels("table", "logs.events", "column", LocalTableFixture.COLUMN));
    assertNotNull(millis, "wall time is keyed by table and column, not by kind");
    assertTrue(millis.sum() > 0, "three builds that read Parquet and wrote leaves took no time at all?");

    String scrape = metrics.scrape();
    assertTrue(scrape.contains(
        "kahshe_table_index_builds_total{table=\"logs.events\",column=\"msg\",kind=\"full\"} 1\n"), scrape);
    assertTrue(scrape.contains(
        "kahshe_table_index_build_seconds_total{table=\"logs.events\",column=\"msg\"} "), scrape);
    assertFalse(scrape.contains("tenant-a"), "the prefix is not a label: " + scrape);
  }
}
