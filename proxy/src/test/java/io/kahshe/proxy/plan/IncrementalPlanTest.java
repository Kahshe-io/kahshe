package io.kahshe.proxy.plan;

import io.kahshe.indexer.BuildConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.Metrics;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.format.type.term.TermIndex;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.rest.PlanStatus;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.proxy.TestConfigs;

/**
 * An incremental scan ({@code start-snapshot-id}) is planned inline, COMPLETED, with every task
 * in the response — never delegated to a plan id that only one replica can resolve.
 *
 * <p>The alternative — iceberg-core's {@code CatalogHandlers} — parks overflow batches in a
 * JVM-global map, so a client whose task fetch landed on another replica would get a 404 and report
 * a failed query.
 */
class IncrementalPlanTest {
  @TempDir Path tmp;

  private static Set<String> locations(PlanTableScanResponse response) {
    return response.fileScanTasks().stream()
        .map(t -> t.file().location())
        .collect(Collectors.toSet());
  }

  @Test
  void anIncrementalRangeIsAnsweredInlineWithExactlyTheFilesAppendedInIt() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Metrics metrics = new Metrics();
    HadoopCatalog catalog = new HadoopCatalog(new Configuration(), tmp.toString());
    TableIdentifier ident = TableIdentifier.of("logs", "events");
    Table table =
        catalog.createTable(
            ident,
            new Schema(Types.NestedField.required(1, LocalTableFixture.COLUMN, Types.StringType.get())));
    String f1 = LocalTableFixture.appendFile(table, "f1.parquet", "alpha");
    long s1 = table.currentSnapshot().snapshotId();
    String f2 = LocalTableFixture.appendFile(table, "f2.parquet", "bravo");
    String f3 = LocalTableFixture.appendFile(table, "f3.parquet", "charlie");
    long s3 = table.currentSnapshot().snapshotId();

    PlanService service = new PlanService(metrics, new TermIndex(config.format(), metrics), TestConfigs.proxyConfig(), config.format());

    PlanTableScanResponse incremental =
        service.plan(
            catalog,
            ident,
            PlanTableScanRequest.builder().withStartSnapshotId(s1).withEndSnapshotId(s3).build(),
            List.of());
    assertEquals(PlanStatus.COMPLETED, incremental.planStatus(), "answered in one response");
    assertTrue(
        incremental.planId().startsWith(PlanService.INLINE_PLAN_ID_PREFIX),
        "an inline id: no replica holds state for it");
    assertEquals(
        Set.of(f2, f3),
        locations(incremental),
        "exactly the files appended after the start snapshot, none from before it");

    // Control: the same table planned without a range returns everything, so the range is what
    // did the excluding.
    PlanTableScanResponse full =
        service.plan(catalog, ident, PlanTableScanRequest.builder().build(), List.of());
    assertEquals(Set.of(f1, f2, f3), locations(full));
  }
}
