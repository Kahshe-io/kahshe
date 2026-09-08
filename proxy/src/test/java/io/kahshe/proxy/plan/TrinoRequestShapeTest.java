package io.kahshe.proxy.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.kahshe.common.Metrics;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.build.IndexBuilder;
import io.kahshe.indexer.LocalTableFixture;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.requests.PlanTableScanRequestParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.proxy.TestConfigs;
import io.kahshe.proxy.catalog.BackendCatalogs;

/**
 * The plan request exactly as Trino 483 sends it for {@code col = 'x'}: an IN with one value,
 * a select list, stats fields, case-sensitive and use-snapshot-schema set.
 *
 * <p>A range collector that refuses a single-value IN answers Trino with a 400 on every equality.
 * Nothing that calls the plan endpoint by hand sends this shape, so only a test pinned to the
 * engine's own serialization catches it. Verified red against a collector that refuses a
 * single-value IN.
 */
class TrinoRequestShapeTest {
  @TempDir Path tmp;

  @Test
  void anEqualityAsTrinoPushesItPrunesToTheFileHoldingTheValue() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Metrics metrics = new Metrics();
    HadoopCatalog catalog = new HadoopCatalog(new Configuration(), tmp.toString());
    TableIdentifier ident = TableIdentifier.of("logs", "httplogs");
    Table table = catalog.createTable(ident,
        new Schema(Types.NestedField.required(1, LocalTableFixture.COLUMN, Types.StringType.get())));
    table.updateProperties().set("kahshe.index." + LocalTableFixture.COLUMN + ".analyzer", "value").commit();
    String f1 = LocalTableFixture.appendFile(table, "f1.parquet", "71.162.18.0", "10.0.0.1");
    LocalTableFixture.appendFile(table, "f2.parquet", "71.162.18.7", "192.168.0.1");
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    table.refresh();

    String body = "{\"snapshot-id\":" + table.currentSnapshot().snapshotId()
        + ",\"select\":[\"" + LocalTableFixture.COLUMN + "\"],"
        + "\"filter\":{\"type\":\"in\",\"term\":\"" + LocalTableFixture.COLUMN + "\",\"values\":[\"71.162.18.0\"]},"
        + "\"case-sensitive\":true,\"use-snapshot-schema\":true,\"stats-fields\":[\"" + LocalTableFixture.COLUMN + "\"]}";
    ContainsExtractor.Extraction extraction = ContainsExtractor.extract(body, name -> true);
    PlanTableScanRequest request = PlanTableScanRequestParser.fromJson(extraction.cleanedJson());
    PlanService service = new PlanService(metrics, new TermIndex(config.format(), metrics), TestConfigs.proxyConfig(), config.format());

    Set<String> kept = service
        .plan(new BackendCatalogs.PlanningCatalog("service|test", catalog), ident, request, extraction.hints(), null)
        .fileScanTasks().stream()
        .map(t -> t.file().location()).collect(Collectors.toSet());
    assertEquals(Set.of(f1), kept, "Trino's single-value IN is an equality on the whole value: one file");
  }
}
