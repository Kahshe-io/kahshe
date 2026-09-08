package io.kahshe.proxy.plan;

import io.kahshe.indexer.BuildConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kahshe.common.Metrics;
import io.kahshe.indexer.build.IndexBuilder;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.format.type.term.TermIndex;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.proxy.TestConfigs;
import io.kahshe.proxy.catalog.BackendCatalogs;

/**
 * {@code _count} by prefix: every term under the prefix, summed, through the route itself rather
 * than the dictionary read alone, because the route's prefix branch hands the term path a null
 * token and only the guard on that admission check keeps it from a 500. Verified red with the
 * guard removed.
 */
class CountByPrefixTest {
  @TempDir Path tmp;

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, LocalTableFixture.COLUMN, Types.StringType.get()),
          Types.NestedField.optional(2, "ip", Types.StringType.get()));

  private static Record row(Schema schema, String ip) {
    GenericRecord r = GenericRecord.create(schema);
    r.setField(LocalTableFixture.COLUMN, "x");
    r.setField("ip", ip);
    return r;
  }

  @Test
  void aPrefixCountSumsTheRunAndACappedOneIsRefused() throws Exception {
    // caller-identity planning (the default), so the route loads the table through the seeded
    // caller catalog
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, SCHEMA, "seed");
    LocalTableFixture.appendRecords(table, "f2.parquet", row(table.schema(), "71.162.18.0"), row(table.schema(), "71.162.18.0"));
    LocalTableFixture.appendRecords(table, "f3.parquet", row(table.schema(), "71.162.200.5"));
    LocalTableFixture.appendRecords(table, "f4.parquet", row(table.schema(), "10.0.0.1"));
    table.updateProperties().set("kahshe.index.ip.analyzer", "value").commit();
    table.refresh();
    IndexBuilder.buildColumn(table, "ip", config);

    BackendCatalogs catalogs = new BackendCatalogs(TestConfigs.proxyConfig());
    Catalog caller = mock(Catalog.class);
    when(caller.loadTable(TableIdentifier.of("logs", "t"))).thenReturn(table);
    catalogs.seedCallerCatalog("lake", "Bearer tok", caller);
    CountRoutes routes = new CountRoutes(config.format(), catalogs, new TermIndex(config.format(), new Metrics()));

    PlanRoutes.Result byPrefix = routes.count("lake", "logs", "t",
        "{\"column\":\"ip\",\"prefix\":\"71.162.\"}".getBytes(StandardCharsets.UTF_8), "Bearer tok");
    assertEquals(200, byPrefix.status(), new String(byPrefix.body(), StandardCharsets.UTF_8));
    JsonNode body = new ObjectMapper().readTree(byPrefix.body());
    assertEquals(3, body.path("count").asLong(), "two of one address and one of another under the /16");
    assertTrue(body.path("exact").asBoolean());
    assertEquals(2, body.path("coverage").path("files_with_term").asInt());

    PlanRoutes.Result byTerm = routes.count("lake", "logs", "t",
        "{\"column\":\"ip\",\"term\":\"71.162.18.0\"}".getBytes(StandardCharsets.UTF_8), "Bearer tok");
    assertEquals(200, byTerm.status());
    assertEquals(2, new ObjectMapper().readTree(byTerm.body()).path("count").asLong());

    PlanRoutes.Result both = routes.count("lake", "logs", "t",
        "{\"column\":\"ip\",\"term\":\"a\",\"prefix\":\"b\"}".getBytes(StandardCharsets.UTF_8), "Bearer tok");
    assertEquals(400, both.status(), "term and prefix are one or the other");

    CountRoutes capped = new CountRoutes(
        LocalTableFixture.withPrefixMaxTerms(config, 1).format(), catalogs,
        new TermIndex(config.format(), new Metrics()));
    PlanRoutes.Result refused = capped.count("lake", "logs", "t",
        "{\"column\":\"ip\",\"prefix\":\"71.\"}".getBytes(StandardCharsets.UTF_8), "Bearer tok");
    assertEquals(422, refused.status(), "a run past the cap is refused, not truncated");
  }
}
