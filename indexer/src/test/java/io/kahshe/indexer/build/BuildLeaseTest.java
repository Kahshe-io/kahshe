package io.kahshe.indexer.build;

import io.kahshe.format.type.gram.Grams;
import io.kahshe.format.BuildLease;
import io.kahshe.format.IndexPaths;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.format.type.bloom.IndexMeta;
import io.kahshe.format.type.term.TermIndexWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.PositionOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;

/**
 * One builder per column: a second build refuses loudly while another owner's lease is live,
 * overrides an abandoned one, and leaves no lease behind when it finishes.
 *
 * <p>Verified red with the acquire-and-release wrapper removed from {@code IndexBuilder}: the
 * held-lease build runs to completion instead of throwing. The shutdown test verified red with
 * {@code BuildLease.releaseAll} releasing nothing: the lease survives the hook.
 */
class BuildLeaseTest {
  @TempDir Path tmp;

  private static String leasePath(Table table, BuildConfig config) {
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    return BuildLease.leasePath(IndexPaths.root(table, config.format().indexRoot()), fieldId);
  }

  private static void writeLease(FileIO io, String path, String owner, long expiresMs)
      throws Exception {
    try (PositionOutputStream out = io.newOutputFile(path).createOrOverwrite()) {
      out.write(
          ("{\"owner\":\"" + owner + "\",\"acquired-ms\":0,\"expires-ms\":" + expiresMs + "}")
              .getBytes(StandardCharsets.UTF_8));
    }
  }

  @Test
  void aLiveLeaseHeldByAnotherBuilderRefusesTheBuildAndNamesTheHolder() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha");
    FileIO io = IndexPaths.io(table, config.format());
    writeLease(io, leasePath(table, config), "other-host/4242/abc", System.currentTimeMillis() + 60_000);

    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () -> IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config),
            "two builds of one column must not run at once");
    assertTrue(e.getMessage().contains("other-host/4242/abc"), e.getMessage());
    assertTrue(
        io.newInputFile(leasePath(table, config)).exists(),
        "the refused build must not delete the lease it does not own");
  }

  @Test
  void anExpiredLeaseIsTreatedAsAbandonedAndTheBuildProceeds() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha");
    FileIO io = IndexPaths.io(table, config.format());
    writeLease(io, leasePath(table, config), "dead-host/1/xyz", System.currentTimeMillis() - 1);

    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));
    assertFalse(
        io.newInputFile(leasePath(table, config)).exists(),
        "a finished build leaves no lease behind, including one it overrode");
  }

  /** A listener that steals the lease the moment the build is about to publish. */
  private static IndexBuildListener thief(FileIO io, String path) {
    return new IndexBuildListener() {
      @Override
      public BuildContext start( String prefix, String namespace, String tableName, String column, long snapshotId, BuildKind buildKind, Set<String> priorCovered, Grams.Contract grams) {
        return NONE.start(prefix, namespace, tableName, column, snapshotId, buildKind, priorCovered, grams);
      }

      @Override
      public void beforePublish() {
        try {
          writeLease(io, path, "thief/1/abc", System.currentTimeMillis() + 60_000);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  @Test
  void aLeaseStolenBeforeTheFirstPublishAbortsABuildThatReadFiles() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha");
    FileIO io = IndexPaths.io(table, config.format());
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    LocalTableFixture.appendFile(table, "f2.parquet", "bravo");
    table.refresh();
    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () -> IndexBuilder.buildColumn(
                table, LocalTableFixture.COLUMN, config, thief(io, leasePath(table, config)), "p", "ns", "t"),
            "the last check before the first publish must see the stolen lease");
    assertTrue(e.getMessage().contains("thief/1/abc"), e.getMessage());
  }

  @Test
  void aLeaseStolenBeforeARestampAbortsItToo() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha");
    String f2 = LocalTableFixture.appendFile(table, "f2.parquet", "bravo");
    table.refresh();
    FileIO io = IndexPaths.io(table, config.format());
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    // a departure and nothing new to read: the restamp path, which publishes both documents
    table.newDelete().deleteFile(f2).commit();
    table.refresh();
    assertThrows(
        IllegalStateException.class,
        () -> IndexBuilder.buildColumn(
            table, LocalTableFixture.COLUMN, config, thief(io, leasePath(table, config)), "p", "ns", "t"),
        "a restamp publishes, so it makes the same last check");
  }

  /** Holds the build at {@code start} — lease taken, nothing published — until told to go on. */
  private static IndexBuildListener heldAt(CountDownLatch reached, CountDownLatch resume) {
    return new IndexBuildListener() {
      @Override
      public BuildContext start(String prefix, String namespace, String tableName, String column,
          long snapshotId, BuildKind buildKind, Set<String> priorCovered, Grams.Contract grams) {
        reached.countDown();
        try {
          resume.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return NOOP_CONTEXT;
      }
    };
  }

  /**
   * The shutdown case: the JVM's hook fires while a build
   * is mid-read. The hook releases the lease the process holds; the build thread is not stopped
   * and runs on to its publish, where the lease check finds nothing and aborts it with nothing
   * written. Its own {@code finally} then releases nothing a second time.
   */
  @Test
  void aBuildInterruptedByShutdownLeavesNoLeaseAndPublishesNothing() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha");
    FileIO io = IndexPaths.io(table, config.format());
    String lease = leasePath(table, config);
    CountDownLatch reached = new CountDownLatch(1);
    CountDownLatch resume = new CountDownLatch(1);
    AtomicReference<Throwable> outcome = new AtomicReference<>();
    Thread build =
        new Thread(
            () -> {
              try {
                IndexBuilder.buildColumn(
                    table, LocalTableFixture.COLUMN, config, heldAt(reached, resume), "p", "ns", "t");
              } catch (Throwable e) {
                outcome.set(e);
              }
            },
            "build-under-test");
    build.start();
    assertTrue(reached.await(30, TimeUnit.SECONDS), "the build did not reach start");
    assertTrue(io.newInputFile(lease).exists(), "a running build holds its lease");

    // what Kahshe's shutdown hook does, while the build thread is still alive
    assertEquals(1, BuildLease.releaseAll(), "this process holds exactly the one lease");
    assertFalse(io.newInputFile(lease).exists(), "the hook must release the lease the build holds");

    resume.countDown();
    build.join(TimeUnit.SECONDS.toMillis(60));
    assertFalse(build.isAlive(), "the build did not finish");
    assertInstanceOf(
        IllegalStateException.class, outcome.get(),
        "the build must abort at its lease check rather than publish under a released lease");
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    String root = IndexPaths.root(table, config.format().indexRoot());
    assertFalse(
        io.newInputFile(IndexMeta.metaPath(root, fieldId)).exists(), "nothing published: bloom");
    assertFalse(
        io.newInputFile(TermIndexWriter.dir(root, fieldId) + "/index-metadata.json").exists(),
        "nothing published: term");
    assertFalse(io.newInputFile(lease).exists(), "and still no lease after the build unwound");
    assertEquals(0, BuildLease.releaseAll(), "the unwound build left nothing registered");
  }
}
