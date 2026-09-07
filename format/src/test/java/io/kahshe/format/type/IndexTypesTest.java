package io.kahshe.format.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.Metrics;
import io.kahshe.format.ext.TestRowIndexType;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.build.IndexBuilder;
import io.kahshe.indexer.LocalTableFixture;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.format.IndexPruner;
import io.kahshe.format.type.bloom.BloomIndexType;
import io.kahshe.format.type.gram.GramIndexType;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.format.type.term.TermIndexType;

/** The registry every build and every plan iterates: the three built-ins plus whatever is added. */
class IndexTypesTest {
  @TempDir Path tmp;

  private static List<String> keys() {
    return IndexTypes.inCostOrder().stream().map(IndexType::key).toList();
  }

  private static IndexPruner pruner(BuildConfig config) {
    Metrics metrics = new Metrics();
    return new IndexPruner(new TermIndex(config.format(), metrics), metrics, config.format());
  }

  private static List<IndexPruner.ContainsHint> contains(String literal) {
    return List.of(
        new IndexPruner.ContainsHint(
            LocalTableFixture.COLUMN, literal, IndexPruner.HintKind.CONTAINS));
  }

  @Test
  void theThreeBuiltInsComeFirstInCostOrder() {
    assertEquals(
        List.of(BloomIndexType.KEY, GramIndexType.KEY, TermIndexType.KEY),
        keys().subList(0, 3));
    assertEquals(List.of("bloom", "grams", "aggregate"), IndexTypes.COST_ORDER);
  }

  @Test
  void aTypeFromAServicesFileIsDiscoveredAfterThem() {
    List<String> keys = keys();
    assertTrue(keys.contains(TestRowIndexType.KEY), keys.toString());
    assertEquals(keys.size() - 1, keys.indexOf(TestRowIndexType.KEY), keys.toString());
  }

  @Test
  void itsCollectorSeesEveryRowAndItsWriteIsCalledAtPublish() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha bravo", "charlie delta", "echo");
    TestRowIndexType.reset();
    assertNotNull(
        IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, LocalTableFixture.config()));

    assertEquals(1, TestRowIndexType.FILES.get(), "one data file opened");
    assertEquals(3, TestRowIndexType.ROWS.get(), "every row of the build");
    assertEquals(5, TestRowIndexType.TOKENS.get(), "the tokens those rows carried");
    assertEquals(1, TestRowIndexType.FILES_DONE.get(), "one data file finished");
    assertEquals(1, TestRowIndexType.WRITES.get(), "written once, at the publish");
  }

  @Test
  void aTypeWhoseLoadAnswersNullPrunesNothing() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha bravo");
    BuildConfig config = LocalTableFixture.config();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    List<FileScanTask> tasks = LocalTableFixture.planTasks(table);
    // "alpha" is present, so the three built-ins keep this file; only the absent fourth type
    // could drop it, and its prune returns nothing
    assertEquals(tasks.size(), pruner(config).prune(table, null, contains("alpha"), tasks).size());
  }

  @Test
  void aTypeWhoseLoadThrowsIsTreatedAsAbsentAndKeepsEveryTask() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha bravo");
    BuildConfig config = LocalTableFixture.config();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    List<FileScanTask> tasks = LocalTableFixture.planTasks(table);
    System.setProperty(TestRowIndexType.MODE, "throw");
    try {
      assertEquals(
          tasks.size(), pruner(config).prune(table, null, contains("alpha"), tasks).size());
    } finally {
      System.clearProperty(TestRowIndexType.MODE);
    }
  }
}
