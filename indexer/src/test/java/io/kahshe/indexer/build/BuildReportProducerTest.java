package io.kahshe.indexer.build;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.format.BuildReport;
import io.kahshe.format.type.gram.Grams;
import io.kahshe.format.IndexPaths;
import io.kahshe.format.type.term.TermIndexWriter;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;

/**
 * Every publish writes the report beside the term metadata: a full build, then an incremental
 * one chained to it by id with the appended file as the one added, then a restamp that changed
 * nothing but the snapshot pointer; and the alerts a listener's context raised ride out in it.
 * Verified red with the chain dropped (previous-build-id null on the second build).
 */
class BuildReportProducerTest {
  @TempDir Path tmp;

  private static BuildReport report(Table table, BuildConfig config) {
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    return BuildReport.read(table.io(), IndexPaths.root(table, config.format().indexRoot()), fieldId);
  }

  @Test
  void fullThenIncrementalThenRestampChainAndDescribeTheirBuilds() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha", "bravo");
    BuildConfig config = LocalTableFixture.config();
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    BuildReport first = report(table, config);
    assertNotNull(first, "a build writes its report");
    assertEquals(BuildReport.Kind.FULL, first.kind());
    assertNull(first.previousBuildId(), "a column's first build chains to nothing");
    assertEquals(1, first.added().size());
    assertEquals(1L, first.counters().get("files-covered"));
    assertEquals("kahshe-grams-v2-n3", first.grams());
    assertTrue(first.leaves().stream().anyMatch(l -> l.contains("/aggregate-")), "names the leaves it published");
    assertTrue(first.publishedMs() >= first.startedMs());

    String f2 = LocalTableFixture.appendFile(table, "f2.parquet", "charlie");
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    BuildReport second = report(table, config);
    assertEquals(BuildReport.Kind.INCREMENTAL, second.kind());
    assertEquals(first.buildId(), second.previousBuildId(), "chained to the build before it");
    assertNotEquals(first.buildId(), second.buildId());
    assertEquals(List.of(f2), second.added());
    assertEquals(2L, second.counters().get("files-covered"));

    table.refresh();
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    BuildReport third = report(table, config);
    assertEquals(BuildReport.Kind.RESTAMP, third.kind(), "nothing new: the snapshot pointer moved, the report says so");
    assertEquals(second.buildId(), third.previousBuildId());
    assertTrue(third.added().isEmpty());
  }

  @Test
  void theAlertsAListenerRaisesRideOutInTheReport() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "disk pressure on node");
    BuildConfig config = LocalTableFixture.config();
    Map<String, Object> alert = Map.of("rule", Map.of("id", "r-disk"), "file", Map.of("path", "x"));
    IndexBuildListener raising = (prefix, ns, t, c, snap, kind, prior, grams) -> new IndexBuildListener.BuildContext() {
      @Override
      public void file(String path, TermIndexWriter.FileTerms terms, Set<String> g) {}

      @Override
      public void done() {}

      @Override
      public List<Map<String, Object>> alerts() {
        return List.of(alert);
      }
    };
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config, raising, "p", "ns", "t");
    BuildReport report = report(table, config);
    assertEquals(1, report.alerts().size());
    assertEquals("r-disk", ((Map<?, ?>) report.alerts().get(0).get("rule")).get("id"));
    assertEquals("p", report.prefix());
    assertEquals(Grams.Contract.current(config.ngram()).id(), report.grams());
  }
}
