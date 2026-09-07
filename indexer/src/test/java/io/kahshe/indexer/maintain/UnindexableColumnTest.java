package io.kahshe.indexer.maintain;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.kahshe.common.Metrics;
import io.kahshe.format.IndexPaths;
import io.kahshe.format.type.term.TermIndexWriter;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.indexer.TableSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * One column {@code kahshe.index} names that kahshe cannot index is a CONFIGURATION error, and it
 * stays contained to that column.
 *
 * <p>The trap: the build refuses such a column with an {@code IllegalArgumentException}, and a
 * per-column loop that lets it escape to the table-level catch un-claims by table key — every
 * sibling after it in rotation order goes unbuilt, the table is never recorded as handled, and
 * every later observation re-runs the identical failing job, behind nothing louder than a WARN.
 *
 * <p>The bad column here is a {@code double}: floating point has no canonical form, so no tier can
 * key on it. The types the build accepts do change — containers are being added — and what is
 * under test is "a column the build refuses", not any particular one, so this fixture uses a type
 * that will not quietly become supported underneath it.
 *
 * <p>Verified red by putting the refusal back on the table's shoulders: the good column never
 * builds ("timed out waiting for the good column to be built") and every observation moves
 * {@code kahshe_index_build_failures_total} for a table whose configuration fails identically
 * every time.
 */
class UnindexableColumnTest {
  @TempDir Path tmp;

  /** {@link LocalTableFixture#COLUMN}, plus a column no index tier has a form for. */
  private Table tableWithADoubleColumn(String... values) throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, LocalTableFixture.COLUMN, Types.StringType.get()),
            Types.NestedField.optional(2, "score", Types.DoubleType.get()));
    return LocalTableFixture.createTable(tmp, schema, values);
  }

  /** Counts loads: a job that re-enters the queue is a job that loads the table again. */
  private static final class CountingSource implements TableSource {
    final AtomicInteger loads = new AtomicInteger();
    private final Table table;

    CountingSource(Table table) {
      this.table = table;
    }

    @Override
    public Table load(String prefix, TableIdentifier ident) {
      loads.incrementAndGet();
      return table;
    }

    @Override
    public void invalidate(String prefix, TableIdentifier ident) {}
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

  @Test
  void theSiblingsOfAnUnindexableColumnAreStillBuiltAndTheTableIsRecordedAsHandled()
      throws Exception {
    Table table = tableWithADoubleColumn("alpha beta", "gamma");
    long snapshot = table.currentSnapshot().snapshotId();
    Metrics metrics = new Metrics();
    CountingSource source = new CountingSource(table);
    IndexerService indexer = new IndexerService(source, metrics, true, LocalTableFixture.config());

    // the bad column FIRST in rotation order: whatever aborts the loop takes the good one with it
    indexer.observe("p", "logs", "events", List.of("score", LocalTableFixture.COLUMN), snapshot);

    await("the good column to be built", () -> metrics.indexBuilds.sum() == 1);
    assertEquals(1, indexer.columnsRefused.sum(), "and the bad one is refused, once");
    assertEquals(
        0,
        metrics.indexBuildFailures.sum(),
        "a column kahshe can never index is configuration, not a build that failed and may pass");
    assertEquals(
        0,
        metrics.indexTablesBehind.getAsLong(),
        "the table is covered as far as it ever can be: a gauge that can never fall is a muted one");

    for (int i = 0; i < 20; i++) {
      indexer.observe("p", "logs", "events", List.of("score", LocalTableFixture.COLUMN), snapshot);
    }
    // A marker job behind those twenty. One worker draining one FIFO queue, so its load lands after
    // anything they enqueued -- which makes the count below an assertion rather than a race.
    indexer.observe("p", "logs", "marker", List.of(LocalTableFixture.COLUMN), snapshot);
    await("the marker job to be drained", () -> source.loads.get() >= 2);
    assertEquals(
        2,
        source.loads.get(),
        "the repeats enqueued nothing: the table was recorded as handled, not retried forever");
  }

  @Test
  void aRefusalIsRememberedSoEveryCommitDoesNotReAttemptIt() throws Exception {
    Table table = tableWithADoubleColumn("alpha beta");
    Metrics metrics = new Metrics();
    IndexerService indexer =
        new IndexerService(new CountingSource(table), metrics, true, LocalTableFixture.config());
    List<String> columns = List.of("score", LocalTableFixture.COLUMN);

    indexer.observe("p", "logs", "events", columns, table.currentSnapshot().snapshotId());
    await("the first build", () -> metrics.indexBuilds.sum() == 1);

    LocalTableFixture.appendFile(table, "f2.parquet", "delta");
    table.refresh();
    indexer.observe("p", "logs", "events", columns, table.currentSnapshot().snapshotId());

    await("the good column to be rebuilt at the new snapshot", () -> metrics.indexBuilds.sum() == 2);
    assertEquals(
        1,
        indexer.columnsRefused.sum(),
        "the schema did not change, so the refusal stands: no fresh attempt per commit");
  }

  /**
   * The other half of the isolation, and the one worth more than the first: a column that failed
   * for a reason a retry could fix is still counted as a failure and still holds the table back.
   * A catch broad enough to contain the bad column would otherwise swallow real build failures
   * into a silent per-column skip — the table would read as maintained while a column stopped
   * being built.
   *
   * <p>The failure here is an index written by a NEWER kahshe, which the build refuses rather than
   * downgrade in place.
   */
  @Test
  void aColumnThatMerelyFailedIsCountedAndTheTableIsRetried() throws Exception {
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
    CountingSource source = new CountingSource(table);
    IndexerService indexer = new IndexerService(source, metrics, true, config);
    List<String> columns = List.of(LocalTableFixture.COLUMN, "other");

    indexer.observe("p", "logs", "events", columns, snapshot);

    await("the sibling to be built anyway", () -> metrics.indexBuilds.sum() == 1);
    await("the failure to be counted", () -> metrics.indexBuildFailures.sum() == 1);
    assertEquals(
        0, indexer.columnsRefused.sum(), "a build that may pass next time is not a refusal");

    // Barriers, not sleeps: a marker job's load proves the job before it in the single worker's
    // FIFO queue has run to the end, which is where the table is un-claimed. Its own column is
    // current by then, so a marker builds nothing and writes nothing.
    indexer.observe("p", "logs", "marker", List.of("other"), snapshot);
    await("the first job to finish", () -> source.loads.get() >= 2);

    indexer.observe("p", "logs", "events", columns, snapshot);
    await("the same observation to be accepted again", () -> source.loads.get() >= 3);
    // and behind the retry, so the temp directory outlives every write it makes
    indexer.observe("p", "logs", "marker2", List.of("other"), snapshot);
    await("the retry to finish", () -> source.loads.get() >= 4);
    assertEquals(1, metrics.indexBuilds.sum(), "the sibling was current, so only its column ran");
  }
}
