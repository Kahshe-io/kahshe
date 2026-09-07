package io.kahshe.proxy.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.kahshe.common.Metrics;
import io.kahshe.common.Records;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.requests.PlanTableScanRequestParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.proxy.ProxyConfig;
import io.kahshe.proxy.TestConfigs;

/**
 * The stats option: by default a plan response carries no per-file column statistics; under
 * {@code requested}, the statistics of exactly the columns the request names in
 * {@code stats-fields} come back and no others; a table property overrides the deployment in
 * either direction; a value nobody defined means strip. The reason to return them at all: without
 * per-file statistics Trino decodes every page of every kept file.
 * Verified red with the requested branch removed: the bounds stayed null.
 */
class PlanStatsOptionTest {
  @TempDir Path tmp;

  private Table table;
  private TableIdentifier ident;
  private HadoopCatalog catalog;
  private int msgId;
  private int otherId;

  private void createTable() throws Exception {
    catalog = new HadoopCatalog(new Configuration(), tmp.toString());
    ident = TableIdentifier.of("logs", "events");
    Schema schema = new Schema(
        Types.NestedField.required(1, LocalTableFixture.COLUMN, Types.StringType.get()),
        Types.NestedField.required(2, "other", Types.StringType.get()));
    table = catalog.createTable(ident, schema);
    GenericRecord r = GenericRecord.create(schema);
    LocalTableFixture.appendRecords(table, "f1.parquet",
        r.copy(Map.of(LocalTableFixture.COLUMN, "alpha", "other", "one")),
        r.copy(Map.of(LocalTableFixture.COLUMN, "bravo", "other", "two")));
    msgId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    otherId = table.schema().findField("other").fieldId();
  }

  private DataFile planned(ProxyConfig proxy, String statsFields) throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Metrics metrics = new Metrics();
    PlanService service = new PlanService(metrics, new TermIndex(config.format(), metrics), proxy, config.format());
    String body = "{\"filter\":{\"type\":\"eq\",\"term\":\"" + LocalTableFixture.COLUMN + "\",\"value\":\"alpha\"}"
        + (statsFields == null ? "" : ",\"stats-fields\":[" + statsFields + "]") + "}";
    PlanTableScanRequest request = PlanTableScanRequestParser.fromJson(body);
    List<org.apache.iceberg.FileScanTask> tasks = service.plan(catalog, ident, request, List.of()).fileScanTasks();
    assertEquals(1, tasks.size());
    return tasks.get(0).file();
  }

  @Test
  void theDefaultStripsEverything() throws Exception {
    createTable();
    DataFile file = planned(TestConfigs.proxyConfig(), "\"" + LocalTableFixture.COLUMN + "\"");
    assertNull(file.lowerBounds(), "strip is the default: no bounds leave the proxy even when asked");
    assertNull(file.valueCounts());
  }

  @Test
  void requestedReturnsTheNamedColumnsStatisticsAndNoOthers() throws Exception {
    createTable();
    ProxyConfig requested = Records.with(TestConfigs.proxyConfig(), Map.of("planStats", "requested"));
    DataFile file = planned(requested, "\"" + LocalTableFixture.COLUMN + "\"");
    assertNotNull(file.lowerBounds(), "the named column's bounds come back");
    assertNotNull(file.lowerBounds().get(msgId), "for the named column");
    assertNull(file.lowerBounds().get(otherId), "and not for a column the request did not name");
    assertNotNull(file.valueCounts().get(msgId));
    assertNull(file.valueCounts().get(otherId));
    // nothing named: nothing returned, whatever the mode
    DataFile unnamed = planned(requested, null);
    assertNull(unnamed.lowerBounds(), "no stats-fields means no statistics");
  }

  @Test
  void aTableOverridesTheDeploymentInEitherDirection() throws Exception {
    createTable();
    table.updateProperties().set("kahshe.plan-stats", "requested").commit();
    DataFile file = planned(TestConfigs.proxyConfig(), "\"" + LocalTableFixture.COLUMN + "\"");
    assertNotNull(file.lowerBounds(), "the table opted in under a stripping deployment");
    table.updateProperties().set("kahshe.plan-stats", "strip").commit();
    ProxyConfig requested = Records.with(TestConfigs.proxyConfig(), Map.of("planStats", "requested"));
    assertNull(planned(requested, "\"" + LocalTableFixture.COLUMN + "\"").lowerBounds(),
        "the table opted out under a returning deployment");
    table.updateProperties().set("kahshe.plan-stats", "everything").commit();
    assertNull(planned(requested, "\"" + LocalTableFixture.COLUMN + "\"").lowerBounds(),
        "a value nobody defined means strip, never a wider disclosure");
  }
}
