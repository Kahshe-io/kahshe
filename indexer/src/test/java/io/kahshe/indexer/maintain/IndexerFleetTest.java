package io.kahshe.indexer.maintain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.Metrics;
import io.kahshe.common.Records;
import io.kahshe.format.BuildLease;
import io.kahshe.format.IndexPaths;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.indexer.TableSource;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.PositionOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A member of an indexer fleet shares the work rather than fighting over it: a column another
 * member holds the build lease on is
 * skipped and counted, not refused, and the table is left unclaimed so a later observation
 * revisits what this member did not cover.
 *
 * <p>Verified red with the {@code config.fleet()} test removed from the catch: the held column
 * counted a build FAILURE, which is the reading that would page someone about a fleet doing
 * exactly what a fleet does.
 */
class IndexerFleetTest {
  @TempDir Path tmp;

  private static TableSource source(Table table) {
    return new TableSource() {
      @Override
      public Table load(String prefix, TableIdentifier ident) {
        return table;
      }

      @Override
      public void invalidate(String prefix, TableIdentifier ident) {}
    };
  }

  /** A live lease held by somebody else, at the column's lease path. */
  private static void leaseHeldByAnother(Table table, BuildConfig config) throws Exception {
    FileIO io = IndexPaths.io(table, config.format());
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    String path = BuildLease.leasePath(IndexPaths.root(table, config.format().indexRoot()), fieldId);
    try (PositionOutputStream out = io.newOutputFile(path).createOrOverwrite()) {
      out.write(("{\"owner\":\"kahshe-indexer-7/1/ab\",\"acquired-ms\":0,\"expires-ms\":"
          + (System.currentTimeMillis() + 600_000) + "}").getBytes(StandardCharsets.UTF_8));
    }
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
  void aFleetMemberSkipsAColumnAnotherMemberHoldsAndCountsIt() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha");
    BuildConfig config = Records.with(LocalTableFixture.config(), Map.of("fleet", true));
    leaseHeldByAnother(table, config);
    Metrics metrics = new Metrics();
    IndexerService indexer = new IndexerService(source(table), metrics, true, config);

    indexer.observe("p", "logs", "events", List.of(LocalTableFixture.COLUMN), 42L);

    await("the skip to be counted", () -> metrics.indexLeaseSkips.sum() == 1);
    assertEquals(0, metrics.indexBuildFailures.sum(),
        "another member building the column is not this member's failure");
    assertEquals(0, metrics.indexBuilds.sum(), "and it is not this member's build either");
    // left unclaimed: the same observation is accepted again rather than deduped away
    indexer.observe("p", "logs", "events", List.of(LocalTableFixture.COLUMN), 42L);
    await("the skipped table to be revisited", () -> metrics.indexLeaseSkips.sum() == 2);
  }

  @Test
  void aLoneBuilderStillRefusesLoudly() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha");
    BuildConfig config = LocalTableFixture.config();
    assertTrue(!config.fleet(), "the default is one builder per column");
    leaseHeldByAnother(table, config);
    Metrics metrics = new Metrics();
    IndexerService indexer = new IndexerService(source(table), metrics, true, config);

    indexer.observe("p", "logs", "events", List.of(LocalTableFixture.COLUMN), 42L);

    await("the refusal to be counted", () -> metrics.indexBuildFailures.sum() == 1);
    assertEquals(0, metrics.indexLeaseSkips.sum(),
        "outside a fleet a held lease is a refusal to look into, not a skip to count");
  }
}
