package io.kahshe.format;

import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.build.IndexBuilder;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.format.type.gram.GramIndex;
import io.kahshe.format.type.bloom.IndexMeta;
import io.kahshe.format.IndexPaths;
import io.kahshe.format.IndexPruner;
import io.kahshe.format.type.term.TermIndex;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.Metrics;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The pruner consults the exact gram layer first; blooms answer only uncovered files. */
class PrunerGramFirstTest {
  @TempDir Path tmp;

  private static List<IndexPruner.ContainsHint> contains(String literal) {
    return List.of(
        new IndexPruner.ContainsHint(LocalTableFixture.COLUMN, literal, IndexPruner.HintKind.CONTAINS));
  }

  private static IndexPruner pruner(BuildConfig config, Metrics metrics) {
    return new IndexPruner(new TermIndex(config.format(), metrics), metrics, config.format());
  }

  private static void deleteBloomIndex(Table table, BuildConfig config) {
    table.io().deleteFile(
        IndexMeta.metaPath(
            IndexPaths.root(table, config.format().indexRoot()),
            table.schema().findField(LocalTableFixture.COLUMN).fieldId()));
  }

  @Test
  void coveredFileAnsweredByGramsAlone() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha bravo");
    BuildConfig config = LocalTableFixture.config();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));
    // no bloom artifacts left: any pruning below can only have come from the gram layer
    deleteBloomIndex(table, config);

    List<FileScanTask> tasks = LocalTableFixture.planTasks(table);
    IndexPruner pruner = pruner(config, new Metrics());
    assertTrue(pruner.prune(table, null, contains("zzqxy"), tasks).isEmpty());
    assertEquals(1, pruner.prune(table, null, contains("alpha"), tasks).size());
  }

  @Test
  void uncoveredOrdinalFallsBackToBlooms() throws Exception {
    // first build without the gram layer, so file 1 stays outside its coverage
    Table table = LocalTableFixture.createTable(tmp, "alpha bravo");
    assertNotNull(
        IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, LocalTableFixture.config(false)));
    String coveredPath = LocalTableFixture.appendFile(table, "f2.parquet", "charlie delta");
    BuildConfig config = LocalTableFixture.config();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    List<FileScanTask> tasks = LocalTableFixture.planTasks(table);
    // blooms present: the uncovered file is bloom-pruned, the covered one gram-pruned
    assertTrue(pruner(config, new Metrics()).prune(table, null, contains("zzqxy"), tasks).isEmpty());

    // blooms gone: the uncovered file must be kept (advisory), the covered one still gram-pruned
    deleteBloomIndex(table, config);
    List<FileScanTask> kept = pruner(config, new Metrics()).prune(table, null, contains("zzqxy"), tasks);
    assertEquals(1, kept.size());
    assertTrue(!kept.get(0).file().location().equals(coveredPath));
  }

  @Test
  void overBudgetLayerIsRefusedAndBloomsStillPrune() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha bravo");
    BuildConfig config = LocalTableFixture.config();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    Metrics metrics = new Metrics();
    IndexPruner pruner = pruner(config, metrics);
    pruner.gramIndex = new GramIndex(config.format(), metrics);
    pruner.gramIndex.maxLoadBytes = 1;

    List<FileScanTask> tasks = LocalTableFixture.planTasks(table);
    assertTrue(pruner.prune(table, null, contains("zzqxy"), tasks).isEmpty());
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    assertNull(pruner.gramIndex.forField(table, fieldId));
    assertEquals(1, metrics.gramTooLarge.sum());
  }
}
