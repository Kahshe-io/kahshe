package io.kahshe.indexer.build;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.kahshe.indexer.build.IndexBuildListener.BuildContext;
import io.kahshe.indexer.build.IndexBuildListener.BuildKind;
import java.nio.file.Path;
import java.util.Set;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.indexer.LocalTableFixture;

/**
 * A listener failure never fails a build — the promise {@link IndexBuildListener} makes. The two
 * hooks the build calls outside {@code start}, {@code readsTermCounts} and {@code beforePublish},
 * are the ones that need guarding: called on the raw listener, a throwing one fails the build.
 */
class ListenerSafetyTest {
  @TempDir Path tmp;

  private static final BuildContext NOOP =
      new BuildContext() {
        @Override
        public void file(String path, io.kahshe.format.type.term.TermIndexWriter.FileTerms terms, Set<String> grams) {}

        @Override
        public void done() {}
      };

  private static IndexBuildListener throwing(boolean atReadsTermCounts, boolean atBeforePublish) {
    return new IndexBuildListener() {
      @Override
      public boolean readsTermCounts() {
        if (atReadsTermCounts) {
          throw new IllegalStateException("listener broke in readsTermCounts");
        }
        return true;
      }

      @Override
      public void beforePublish() {
        if (atBeforePublish) {
          throw new IllegalStateException("listener broke in beforePublish");
        }
      }

      @Override
      public BuildContext start(
          String prefix, String namespace, String tableName, String column, long snapshotId,
          BuildKind buildKind, Set<String> priorCovered, io.kahshe.format.type.gram.Grams.Contract grams) {
        return NOOP;
      }
    };
  }

  @Test
  void aListenerThatThrowsBeforePublishDoesNotFailTheBuild() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha");
    assertNotNull(
        IndexBuilder.buildColumn(
            table, LocalTableFixture.COLUMN, LocalTableFixture.config(), throwing(false, true),
            "p", "ns", "t"),
        "the build must publish although the listener threw at beforePublish");
  }

  @Test
  void aListenerThatThrowsAtReadsTermCountsDoesNotFailTheBuild() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha");
    assertNotNull(
        IndexBuilder.buildColumn(
            table, LocalTableFixture.COLUMN, LocalTableFixture.config(), throwing(true, false),
            "p", "ns", "t"),
        "the build must run although the listener threw at readsTermCounts");
  }
}
