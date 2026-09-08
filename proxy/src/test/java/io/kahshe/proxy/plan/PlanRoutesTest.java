package io.kahshe.proxy.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.kahshe.common.Metrics;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.proxy.TestConfigs;
import io.kahshe.proxy.catalog.BackendCatalogs;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Which client a plan route reaches for, and when. Under caller identity — the default — reaching
 * for one is a network initialize per fresh token, so the routes that never load a table must not,
 * a submit with no bearer must be refused rather than planned as the service, the client must be
 * keyed on the prefix as decoded (the form the observed-snapshot map and the seed use), and a
 * backend that refuses the initialize must answer with its own status. Each is verified red in
 * the test's own paragraph; the backend behind {@code TestConfigs.proxyConfig()} is
 * {@code http://localhost:0}, so any client actually built here fails loudly.
 */
class PlanRoutesTest {
  @TempDir Path tmp;

  private static final String BASE = "/v1/lake/namespaces/logs/tables/events";
  private static final byte[] EMPTY_PLAN = "{}".getBytes(StandardCharsets.UTF_8);

  private static PlanRoutes routes(BackendCatalogs catalogs) {
    BuildConfig config = LocalTableFixture.config();
    Metrics metrics = new Metrics();
    return new PlanRoutes(
        TestConfigs.proxyConfig(), config.format(), catalogs, metrics,
        new TermIndex(config.format(), metrics));
  }

  /**
   * Fetch, cancel and tasks are decided by the id's shape and carry no identity. Verified red with
   * the client chosen above the switch, as it was: each of the three initialized a caller catalog
   * against port 0 and threw.
   */
  @Test
  void aFetchACancelAndATasksRequestTouchNoCatalog() {
    PlanRoutes routes = routes(new BackendCatalogs(TestConfigs.proxyConfig()));
    assertEquals(404, routes.handle(routes.match("GET", BASE + "/plan/x"), new byte[0], "Bearer t").status());
    assertEquals(204, routes.handle(routes.match("DELETE", BASE + "/plan/sync-1"), new byte[0], "Bearer t").status());
    assertEquals(404, routes.handle(routes.match("POST", BASE + "/tasks"), EMPTY_PLAN, "Bearer t").status());
  }

  /**
   * The gate answers 401 for a missing header before a route runs, so this is the twin guard for
   * anything reaching the route directly. Verified red with the {@code callerToken != null}
   * fallback to the service client restored: the request planned as kahshe.
   */
  @Test
  void aSubmitWithoutABearerUnderCallerIdentityIs401() {
    PlanRoutes routes = routes(new BackendCatalogs(TestConfigs.proxyConfig()));
    PlanRoutes.Result result = routes.handle(routes.match("POST", BASE + "/plan"), EMPTY_PLAN, null);
    assertEquals(401, result.status(), new String(result.body(), StandardCharsets.UTF_8));
  }

  /**
   * The caller client is keyed and warehoused on the decoded prefix, as the service client and the
   * observed-snapshot map already were. Verified red with the raw segment handed to
   * {@code forCaller}: a Nessie-style prefix missed its own seed and initialized a second client.
   */
  @Test
  void aSubmitReachesTheCallerCatalogSeededUnderTheDecodedPrefix() throws Exception {
    HadoopCatalog hadoop = new HadoopCatalog(new Configuration(), tmp.toString());
    TableIdentifier ident = TableIdentifier.of("logs", "t");
    Table table = hadoop.createTable(ident,
        new Schema(Types.NestedField.required(1, LocalTableFixture.COLUMN, Types.StringType.get())));
    LocalTableFixture.appendFile(table, "f1.parquet", "alpha");
    Catalog caller = mock(Catalog.class);
    when(caller.loadTable(ident)).thenReturn(table);
    BackendCatalogs catalogs = new BackendCatalogs(TestConfigs.proxyConfig());
    catalogs.seedCallerCatalog("main|wh", "Bearer tok", caller);
    PlanRoutes routes = routes(catalogs);

    PlanRoutes.Result result =
        routes.handle(routes.match("POST", "/v1/main%7Cwh/namespaces/logs/tables/t/plan"), EMPTY_PLAN, "Bearer tok");
    assertEquals(200, result.status(), new String(result.body(), StandardCharsets.UTF_8));
  }

  /**
   * A refusal from the backend while the caller's client initializes is that refusal, not a 500.
   * An engine re-authenticates on 401 and retries a 500. Verified red with the client chosen
   * outside the try: the exception escaped the route.
   */
  @Test
  void aRefusedInitializeIsTheBackendsStatusNotA500() {
    BackendCatalogs refusing = mock(BackendCatalogs.class);
    when(refusing.forPlanning(any(), any())).thenThrow(new NotAuthorizedException("token expired"));
    PlanRoutes routes = routes(refusing);
    PlanRoutes.Result result =
        routes.handle(routes.match("POST", BASE + "/plan"), EMPTY_PLAN, "Bearer stale");
    assertEquals(401, result.status(), new String(result.body(), StandardCharsets.UTF_8));
  }
}
