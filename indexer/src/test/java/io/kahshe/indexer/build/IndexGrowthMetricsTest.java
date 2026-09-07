package io.kahshe.indexer.build;

import io.kahshe.format.type.bloom.IndexStore;
import io.kahshe.format.type.term.TermIndex;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.Metrics;
import java.nio.file.Path;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;

/**
 * The two index structures that only ever grow must be visible while they do it.
 *
 * <p>Neither shows up in any other signal. A table whose coverage is mostly tombstones and one
 * whose bloom leaf list has an entry per incremental build ever run both serve correct answers, at
 * a steadily rising cold-load cost, without moving a counter or logging anything. That is the shape
 * of failure nothing surfaces unless somebody goes and looks — so these are gauges, and these tests
 * assert they MOVE rather than that they exist.
 */
class IndexGrowthMetricsTest {
  @TempDir Path tmp;

  private static int fieldId(Table table) {
    return table.schema().findField(LocalTableFixture.COLUMN).fieldId();
  }

  /**
   * Verified by breaking it: reporting {@code coverage.size()} instead of the dead count, or
   * dropping the {@code deadPercent.put}, leaves this at 0 and the test fails.
   */
  @Test
  void theDeadOrdinalGaugeRisesWhenAFileLeavesTheTable() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha alpha");
    String f2 = LocalTableFixture.appendFile(table, "f2.parquet", "bravo bravo");
    LocalTableFixture.appendFile(table, "f3.parquet", "charlie charlie");
    LocalTableFixture.appendFile(table, "f4.parquet", "delta delta");
    table.refresh();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    Metrics clean = new Metrics();
    TermIndex cleanReader = new TermIndex(config.format(), clean);
    assertNotNull(cleanReader.forField(table, fieldId(table)));
    assertEquals(
        0, clean.indexDeadOrdinalPercent.getAsLong(), "a freshly built index has no dead ordinals");

    // one of four files leaves, and a fifth arrives so the build has something to do
    table.newDelete().deleteFile(f2).commit();
    table.refresh();
    LocalTableFixture.appendFile(table, "f5.parquet", "echo echo");
    table.refresh();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    Metrics after = new Metrics();
    TermIndex reader = new TermIndex(config.format(), after);
    assertNotNull(reader.forField(table, fieldId(table)));
    // one dead of five recorded entries
    assertEquals(
        20,
        after.indexDeadOrdinalPercent.getAsLong(),
        "coverage carries a tombstone but the gauge does not show it; a table that has rolled "
            + "entirely over would look identical to a fresh one");
  }

  /**
   * The bloom leaf list is append-only — {@code writeBloomLeaf} carries the prior generation's list
   * forward and adds one — and {@code IndexStore.load} opens every entry on a cold read. This
   * asserts the count rises across incremental builds, which is the growth nothing reclaims.
   */
  @Test
  void theBloomLeafGaugeRisesWithEachIncrementalBuild() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha alpha");
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    Metrics first = new Metrics();
    assertNotNull(new IndexStore(config.format(), first).forColumn(table, LocalTableFixture.COLUMN));
    long afterOne = first.indexBloomLeaves.getAsLong();
    assertEquals(1, afterOne, "one build should have written exactly one bloom leaf");

    LocalTableFixture.appendFile(table, "f2.parquet", "bravo bravo");
    table.refresh();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    Metrics second = new Metrics();
    assertNotNull(new IndexStore(config.format(), second).forColumn(table, LocalTableFixture.COLUMN));
    assertTrue(
        second.indexBloomLeaves.getAsLong() > afterOne,
        "the bloom leaf list grew but the gauge did not: an actively maintained table gains one "
            + "leaf per build forever and pays for all of them on every cold load");
  }

  /** Both gauges must appear in the scrape, or nothing can alert on them. */
  @Test
  void bothGaugesAreExposedOnTheMetricsEndpoint() {
    String text = new Metrics().scrape();
    assertTrue(text.contains("kahshe_index_dead_ordinal_percent"), text);
    assertTrue(text.contains("kahshe_index_bloom_leaves"), text);
  }
}
