package io.kahshe.indexer.build;

import io.kahshe.format.IndexPaths;
import io.kahshe.format.type.term.TermIndexWriter;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.indexer.RecordingListener;

/** Locks in the listener wiring invariants of {@link IndexBuilder#buildColumn}. */
class IndexBuilderListenerTest {
  @TempDir Path tmp;

  private static long[] build(Table table, BuildConfig config, RecordingListener listener)
      throws Exception {
    return IndexBuilder.buildColumn(
        table, LocalTableFixture.COLUMN, config, listener, "p", "ns", "t");
  }

  @Test
  void freshBuildReportsFullWithEmptyPriorCoverage() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "hello world", "goodbye moon");
    BuildConfig config = LocalTableFixture.config();
    RecordingListener listener = new RecordingListener();
    assertNotNull(build(table, config, listener));
    assertEquals(1, listener.starts);
    assertEquals(IndexBuildListener.BuildKind.FULL, listener.kind);
    assertTrue(listener.priorCovered.isEmpty());
    assertEquals(1, listener.files.size());
    assertEquals(1, listener.dones);
  }

  @Test
  void incrementalBuildReportsPriorCoverageAndOnlyNewFiles() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "hello world");
    BuildConfig config = LocalTableFixture.config();
    RecordingListener first = new RecordingListener();
    assertNotNull(build(table, config, first));
    String firstPath = first.files.get(0);

    String secondPath = LocalTableFixture.appendFile(table, "f2.parquet", "goodbye moon");
    RecordingListener listener = new RecordingListener();
    assertNotNull(build(table, config, listener));
    assertEquals(1, listener.starts);
    assertEquals(IndexBuildListener.BuildKind.INCREMENTAL, listener.kind);
    assertEquals(Set.of(firstPath), listener.priorCovered);
    assertEquals(List.of(secondPath), listener.files);
    assertEquals(1, listener.dones);
  }

  @Test
  void unreadablePriorLeavesFallBackToFullKeepingPriorCoverage() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "hello world");
    BuildConfig config = LocalTableFixture.config();
    RecordingListener first = new RecordingListener();
    long[] result = build(table, config, first);
    assertNotNull(result);
    String firstPath = first.files.get(0);

    LocalTableFixture.appendFile(table, "f2.parquet", "goodbye moon");
    // delete the term index's aggregate leaf: the incremental path is chosen, then reading prior
    // state fails and the build falls back to a full rebuild
    String root = IndexPaths.root(table, config.format().indexRoot());
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    // Delete every aggregate range leaf. The aggregate is partitioned, so removing one
    // hard-coded name leaves the rest readable and the build stays INCREMENTAL -- which is what
    // this test would then silently stop testing.
    try (var listing = java.nio.file.Files.list(java.nio.file.Path.of(TermIndexWriter.dir(root, fieldId)))) {
      for (java.nio.file.Path leaf : listing.toList()) {
        if (leaf.getFileName().toString().startsWith("aggregate-")) {
          java.nio.file.Files.delete(leaf);
        }
      }
    }

    RecordingListener listener = new RecordingListener();
    assertNotNull(build(table, config, listener));
    assertEquals(IndexBuildListener.BuildKind.FULL, listener.kind);
    // priorCovered is captured before the fallback clears the covered list
    assertEquals(Set.of(firstPath), listener.priorCovered);
    assertEquals(2, listener.files.size());
    assertTrue(listener.files.contains(firstPath));
    assertEquals(1, listener.dones);

    // The PUBLISHED coverage, which nothing above reads. A fallback that flips to a full rebuild
    // AFTER coverage has been derived under the incremental answer leaves the prior entry for f1
    // beside f1's fresh ordinal -- two live ordinals for one path, persisting across every later
    // build, and "nothing to do" on a table with a file the index never read. One live entry per
    // path, and ordinals that restarted, is what a full rebuild means.
    String metaPath = TermIndexWriter.dir(root, fieldId) + "/index-metadata.json";
    com.fasterxml.jackson.databind.JsonNode snapshot;
    try (var in = table.io().newInputFile(metaPath).newStream()) {
      snapshot = TermIndexWriter.snapshotNode(
          new com.fasterxml.jackson.databind.ObjectMapper().readTree(in));
    }
    List<io.kahshe.format.Coverage.Entry> published =
        io.kahshe.format.Coverage.parse(snapshot.path("files"));
    List<String> live = io.kahshe.format.Coverage.livePaths(published);
    assertEquals(2, live.size(), "one live entry per data file: " + published);
    assertEquals(2, Set.copyOf(live).size(), "no path appears twice: " + published);
    assertEquals(2, io.kahshe.format.Coverage.nextOrdinal(published),
        "a full rebuild restarts ordinals; a phantom would push this to 3");
  }
}
