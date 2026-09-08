package io.kahshe.proxy.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.kahshe.common.Metrics;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.proxy.TestConfigs;
import io.kahshe.proxy.catalog.BackendCatalogs;
import java.nio.file.Path;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A plan built through one identity's client is never served to another.
 *
 * <p>The plan cache was keyed by table location alone, so under caller identity a plan built with
 * caller A's client — the manifests A's own credentials could read — answered caller B from cache,
 * whose client had read nothing. Nothing logged it and nothing counted it: B received a complete,
 * correct file list it was not authorised to derive, and the docs said the opposite. Verified red
 * with the key reduced to the location again: the second identity hit instead of missing.
 */
class PlanIdentityTest {
  @TempDir Path tmp;

  @Test
  void aPlanBuiltUnderOneIdentityIsNotServedUnderAnother() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Metrics metrics = new Metrics();
    HadoopCatalog catalog = new HadoopCatalog(new Configuration(), tmp.toString());
    TableIdentifier ident = TableIdentifier.of("logs", "events");
    Table table = catalog.createTable(ident,
        new Schema(Types.NestedField.required(1, LocalTableFixture.COLUMN, Types.StringType.get())));
    LocalTableFixture.appendFile(table, "f1.parquet", "alpha");
    PlanService service = new PlanService(
        metrics, new TermIndex(config.format(), metrics), TestConfigs.proxyConfig(), config.format());
    PlanTableScanRequest request = PlanTableScanRequest.builder().build();
    // Two identities over one catalog: what differs is who read the manifests, not what they hold.
    BackendCatalogs.PlanningCatalog a = new BackendCatalogs.PlanningCatalog("hash-a|lake", catalog);
    BackendCatalogs.PlanningCatalog b = new BackendCatalogs.PlanningCatalog("hash-b|lake", catalog);

    service.plan(a, ident, request, List.of(), "Bearer a");
    service.plan(b, ident, request, List.of(), "Bearer b");
    assertEquals(2, metrics.planCacheMisses.sum(), "the second identity must read the manifests itself");
    assertEquals(0, metrics.planCacheHits.sum(), "a plan built under one identity was served to another");

    service.plan(a, ident, request, List.of(), "Bearer a");
    assertEquals(1, metrics.planCacheHits.sum(), "the same identity is still served from its own entry");
    assertEquals(2, metrics.planCacheMisses.sum());
  }
}
