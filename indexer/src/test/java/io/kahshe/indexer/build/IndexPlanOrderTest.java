package io.kahshe.indexer.build;

import io.kahshe.format.IndexPaths;
import io.kahshe.format.type.term.TermIndexWriter;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;

/**
 * A data file's ordinal must be a function of the table's file SET, not of planning order.
 *
 * <p>The ordinal is the join key between the coverage list and every bitmap in the gram and term
 * tiers. Iceberg plans manifests concurrently, so {@code planFiles()} returns files in an order
 * that varies between runs — so numbering by planning order lets two full builds of one table
 * number its files differently. A bitmap read against the wrong numbering names the wrong files,
 * and naming the wrong files means pruning away files that match: a false negative, the one
 * failure this index is not allowed to have.
 *
 * <p>The hazard is latent while one process owns the index, because an incremental build preserves
 * the prior order and a full rebuild replaces every tier at once. It stops being latent as soon as
 * two processes build parts of one index.
 */
class IndexPlanOrderTest {
  @TempDir Path tmp;

  @Test
  void ordinalsComeFromSortedPathsNotFromAppendOrder() throws Exception {
    // appended in an order that is deliberately NOT sorted: f10 sorts before f2 as a string
    Table table = LocalTableFixture.createTable(tmp, "alpha");
    LocalTableFixture.appendFile(table, "f10.parquet", "bravo");
    LocalTableFixture.appendFile(table, "f2.parquet", "charlie");
    LocalTableFixture.appendFile(table, "f1.parquet", "delta");
    table.refresh();

    BuildConfig config = LocalTableFixture.config();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    List<String> covered = coveredFiles(table, config);
    List<String> sorted = new ArrayList<>(covered);
    java.util.Collections.sort(sorted);
    assertEquals(
        sorted, covered,
        "coverage is not in sorted path order, so an ordinal depends on how the scan was planned");
  }

  @Test
  void twoFullBuildsOfOneTableNumberItsFilesIdentically() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha");
    for (String name : new String[] {"f7.parquet", "f3.parquet", "f11.parquet", "f5.parquet"}) {
      LocalTableFixture.appendFile(table, name, "row for " + name);
    }
    table.refresh();
    BuildConfig config = LocalTableFixture.config();

    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));
    List<String> first = coveredFiles(table, config);

    // wipe and rebuild from nothing: a second full build must agree with the first
    Path root = Path.of(IndexPaths.root(table, config.format().indexRoot()));
    try (var walk = java.nio.file.Files.walk(root)) {
      for (Path path : walk.sorted(java.util.Comparator.reverseOrder()).toList()) {
        java.nio.file.Files.delete(path);
      }
    }
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    assertEquals(
        first, coveredFiles(table, config),
        "two full builds numbered the same table's files differently; every bitmap in the index is "
            + "keyed by that numbering");
  }

  private static List<String> coveredFiles(Table table, BuildConfig config) throws Exception {
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    Path meta =
        Path.of(
            TermIndexWriter.dir(IndexPaths.root(table, config.format().indexRoot()), fieldId)
                + "/index-metadata.json");
    var node =
        TermIndexWriter.snapshotNode(
            new com.fasterxml.jackson.databind.ObjectMapper().readTree(meta.toFile()));
    List<String> files = new ArrayList<>();
    node.path("files").forEach(f -> files.add(f.asText()));
    return files;
  }
}
