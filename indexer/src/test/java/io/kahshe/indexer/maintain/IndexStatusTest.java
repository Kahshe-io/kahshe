package io.kahshe.indexer.maintain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.kahshe.common.Metrics;
import io.kahshe.format.IndexPaths;
import io.kahshe.format.type.term.TermIndexWriter;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.indexer.TableSource;
import io.kahshe.indexer.build.IndexBuilder;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The per-table status document, at its source: what the indexer knows about a table it built,
 * what it says when it cannot build, and what the CLI reads back from storage without any of it.
 *
 * <p>Verified red with {@link IndexerService#tables()} returning an empty list and
 * {@link IndexStatus#fromStorage} answering the "nothing known" document: every test here fails
 * on its first assertion about the table.
 */
class IndexStatusTest {
  @TempDir Path tmp;

  /** Released to let a parked load through; released on teardown so no worker outlives the dir. */
  private final CountDownLatch gate = new CountDownLatch(1);
  private volatile boolean park;

  /** A source that hands out one table, and parks inside {@code load} while asked to. */
  private TableSource source(Table table) {
    return new TableSource() {
      @Override
      public Table load(String prefix, TableIdentifier ident) {
        if (park) {
          try {
            gate.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        return table;
      }

      @Override
      public void invalidate(String prefix, TableIdentifier ident) {}
    };
  }

  @AfterEach
  void unpark() {
    gate.countDown();
  }

  private static void await(String what, BooleanSupplier condition) throws InterruptedException {
    long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(25);
    }
    throw new AssertionError("timed out waiting for " + what);
  }

  private static IndexStatus only(IndexerService indexer) {
    List<IndexStatus> tables = indexer.tables();
    assertEquals(1, tables.size(), "one table observed, one listed: " + tables);
    return tables.get(0);
  }

  private static boolean indexedAt(IndexerService indexer, long snapshot) {
    List<IndexStatus> tables = indexer.tables();
    return tables.size() == 1
        && tables.get(0).indexedSnapshot() != null
        && tables.get(0).indexedSnapshot() == snapshot;
  }

  @Test
  void aDisabledIndexerListsNothingAndSaysWhy() {
    IndexerService indexer =
        new IndexerService(
            LocalTableFixture.noTables(), new Metrics(), false, LocalTableFixture.config());
    indexer.observe("p", "logs", "events", List.of(LocalTableFixture.COLUMN), 7);

    assertTrue(indexer.tables().isEmpty(), "a process that builds nothing has no status to give");
    assertNotNull(indexer.note(), "and must say so rather than read as 'no tables declare an index'");
    assertTrue(indexer.note().contains("KAHSHE_INDEXER"), indexer.note());
    ObjectNode listing = IndexStatus.listing(indexer);
    assertEquals(0, listing.path("tables").size());
    assertEquals(indexer.note(), listing.path("note").asText());
  }

  @Test
  void behindSecondsMeasuresFromTheFirstUncoveredObservation() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha beta", "gamma");
    long first = table.currentSnapshot().snapshotId();
    Metrics metrics = new Metrics();
    IndexerService indexer =
        new IndexerService(source(table), metrics, true, LocalTableFixture.config());
    AtomicLong clock = new AtomicLong(System.currentTimeMillis());
    indexer.freshness.nowMs = clock::get;
    List<String> columns = List.of(LocalTableFixture.COLUMN);

    indexer.observe("p", "logs", "events", columns, first);
    await("the first build", () -> indexedAt(indexer, first));
    IndexStatus built = only(indexer);
    assertEquals("p", built.prefix());
    assertEquals("logs", built.namespace());
    assertEquals("events", built.table());
    assertEquals(columns, built.declaredColumns());
    assertEquals(first, built.currentSnapshot());
    assertEquals(0, built.behindSeconds());
    IndexStatus.Column msg = built.columns().get(LocalTableFixture.COLUMN);
    assertNotNull(msg, "the built column is listed: " + built.columns());
    assertEquals("FULL", msg.kind());
    assertEquals(first, msg.snapshot());
    assertEquals(1, msg.filesCovered());
    assertEquals(1, msg.filesAdded());
    assertNull(built.lastFailure());
    assertTrue(built.refused().isEmpty());

    // a commit the worker cannot yet cover: its load is parked
    park = true;
    LocalTableFixture.appendFile(table, "f2.parquet", "delta");
    table.refresh();
    long second = table.currentSnapshot().snapshotId();
    indexer.observe("p", "logs", "events", columns, second);
    clock.addAndGet(90_000);

    IndexStatus behind = only(indexer);
    assertEquals(second, behind.currentSnapshot());
    assertEquals(first, behind.indexedSnapshot());
    assertEquals(90, behind.behindSeconds(), "dated from the observation, not from now");
    assertEquals(first, behind.columns().get(LocalTableFixture.COLUMN).snapshot(),
        "the column still says what was last built");
    assertThrows(
        UnsupportedOperationException.class,
        () -> indexer.freshness.entries().clear(),
        "the tracked set is handed out as a copy nobody can change");

    gate.countDown();
    await("the second build", () -> indexedAt(indexer, second));
    IndexStatus caughtUp = only(indexer);
    assertEquals(0, caughtUp.behindSeconds());
    assertEquals("INCREMENTAL", caughtUp.columns().get(LocalTableFixture.COLUMN).kind());
    assertEquals(2, caughtUp.columns().get(LocalTableFixture.COLUMN).filesCovered());
  }

  /** {@link LocalTableFixture#COLUMN}, plus a column no index tier has a form for. */
  private Table tableWithADoubleColumn(String... values) throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, LocalTableFixture.COLUMN, Types.StringType.get()),
            Types.NestedField.optional(2, "score", Types.DoubleType.get()));
    return LocalTableFixture.createTable(tmp, schema, values);
  }

  @Test
  void aRefusedColumnIsListedWithItsReason() throws Exception {
    Table table = tableWithADoubleColumn("alpha beta");
    long snapshot = table.currentSnapshot().snapshotId();
    Metrics metrics = new Metrics();
    IndexerService indexer =
        new IndexerService(source(table), metrics, true, LocalTableFixture.config());
    List<String> columns = List.of("score", LocalTableFixture.COLUMN);

    indexer.observe("p", "logs", "events", columns, snapshot);
    await("the good column to be built", () -> indexedAt(indexer, snapshot));

    IndexStatus status = only(indexer);
    assertEquals(columns, status.declaredColumns(), "declared as written, refused or not");
    assertEquals(1, status.refused().size(), status.refused().toString());
    assertEquals("score", status.refused().get(0).column());
    assertFalse(status.refused().get(0).reason().isBlank(), "a refusal names its reason");
    assertEquals(
        List.of(LocalTableFixture.COLUMN), List.copyOf(status.columns().keySet()),
        "only the indexable column has a build");
    assertNull(status.lastFailure(), "a refusal is configuration, not a failure");
  }

  /** The failure here is an index written by a NEWER kahshe, which the build refuses to downgrade. */
  @Test
  void aFailedBuildIsTheTablesLastFailure() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, LocalTableFixture.COLUMN, Types.StringType.get()),
            Types.NestedField.optional(2, "other", Types.StringType.get()));
    Table table = LocalTableFixture.createTable(tmp, schema, "alpha beta");
    long snapshot = table.currentSnapshot().snapshotId();
    BuildConfig config = LocalTableFixture.config();
    String path =
        TermIndexWriter.dir(IndexPaths.root(table, config.format().indexRoot()), 1)
            + "/index-metadata.json";
    try (PositionOutputStream out =
        IndexPaths.io(table, config.format()).newOutputFile(path).createOrOverwrite()) {
      out.write(
          "{\"format-version\":9,\"properties\":{\"kahshe.format-version\":\"9\"}}"
              .getBytes(StandardCharsets.UTF_8));
    }
    Metrics metrics = new Metrics();
    IndexerService indexer = new IndexerService(source(table), metrics, true, config);

    indexer.observe("p", "logs", "events", List.of(LocalTableFixture.COLUMN, "other"), snapshot);
    await("the failure to be counted", () -> metrics.indexBuildFailures.sum() == 1);
    await("the failure to be recorded", () -> only(indexer).lastFailure() != null);
    await("the sibling to be built anyway", () -> metrics.indexBuilds.sum() == 1);

    IndexStatus status = only(indexer);
    assertTrue(
        status.lastFailure().message().contains(LocalTableFixture.COLUMN),
        "the failure names its column: " + status.lastFailure().message());
    assertTrue(status.lastFailure().atMs() > 0);
    assertNull(status.indexedSnapshot(), "a failed pass covers nothing");
    assertNotNull(status.columns().get("other"), "and the sibling that built is still listed");
  }

  /**
   * The CLI's path: no observation, no worker, no memory — the catalog, the build reports and
   * the snapshot history have to give the same answers.
   */
  @Test
  void theCliReadsTheSameStatusFromStorage() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha beta", "gamma");
    table.updateProperties().set(IndexStatus.PROPERTY, LocalTableFixture.COLUMN).commit();
    long first = table.currentSnapshot().snapshotId();
    BuildConfig config = LocalTableFixture.config();
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);

    IndexStatus built =
        IndexStatus.fromStorage(table, "p", "logs", "events", config, System.currentTimeMillis());
    assertEquals(List.of(LocalTableFixture.COLUMN), built.declaredColumns());
    assertEquals(first, built.currentSnapshot());
    assertEquals(first, built.indexedSnapshot());
    assertEquals(0, built.behindSeconds());
    IndexStatus.Column msg = built.columns().get(LocalTableFixture.COLUMN);
    assertNotNull(msg, "the build report is the column's status: " + built.columns());
    assertEquals("FULL", msg.kind());
    assertEquals(1, msg.filesCovered());

    LocalTableFixture.appendFile(table, "f2.parquet", "delta");
    table.refresh();
    long second = table.currentSnapshot().snapshotId();
    IndexStatus behind =
        IndexStatus.fromStorage(table, "p", "logs", "events", config, System.currentTimeMillis());
    assertEquals(second, behind.currentSnapshot());
    assertEquals(first, behind.indexedSnapshot());
    assertTrue(behind.behindSeconds() >= 1, "the uncovered commit has an age: " + behind);

    // every field the document carries says where a storage read got it
    ObjectNode json = behind.toJson(IndexStatus.storageSources("http://backend", "/root"));
    ObjectNode sources = (ObjectNode) json.path("sources");
    for (String field : List.of("declared_columns", "current_snapshot", "indexed_snapshot",
        "behind_seconds", "columns", "last_failure", "refused")) {
      assertTrue(sources.hasNonNull(field), "no source given for " + field + ": " + sources);
    }
  }

  @Test
  void aTableWithNothingIndexableIsNotBehindFromStorage() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    long anHourOn = System.currentTimeMillis() + 3_600_000;
    Table undeclared = LocalTableFixture.createTable(tmp.resolve("undeclared"), "alpha");
    IndexStatus none = IndexStatus.fromStorage(undeclared, "p", "logs", "events", config, anHourOn);
    assertEquals(List.of(), none.declaredColumns());
    assertEquals(0, none.behindSeconds(), "nothing declared: an hour on, still not behind");

    Table refusedOnly = tableWithADoubleColumn("alpha beta");
    refusedOnly.updateProperties().set(IndexStatus.PROPERTY, "score").commit();
    IndexStatus refused =
        IndexStatus.fromStorage(refusedOnly, "p", "logs", "events", config, anHourOn);
    assertEquals(1, refused.refused().size(), refused.refused().toString());
    assertNull(refused.indexedSnapshot(), "no complete build to name");
    assertEquals(0, refused.behindSeconds(),
        "every declared column refused: claimed like a built one in-process, so not behind here");
  }
}
