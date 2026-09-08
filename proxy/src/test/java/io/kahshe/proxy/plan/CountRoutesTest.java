package io.kahshe.proxy.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import io.kahshe.common.Metrics;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.proxy.TestConfigs;
import io.kahshe.proxy.catalog.BackendCatalogs;
import java.nio.charset.StandardCharsets;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.junit.jupiter.api.Test;

/**
 * A refusal from the caller's own client on {@code _count} is relayed with the backend's meaning.
 *
 * <p>The gate in front of the route memoizes a positive verdict for up to the auth TTL, so a
 * token revoked inside that window still reaches the route, and under caller identity the load
 * that follows is the caller's own client refusing. Unmapped, that reached the dispatcher as a
 * 500 — the twin of the mapping the plan route already had, unreachable under service identity
 * and ordinary under the default. Verified red with the load outside its try: both propagated.
 */
class CountRoutesTest {
  @Test
  void aCountForATableTheCallersTokenCannotLoadRelaysTheRefusal() {
    BuildConfig config = LocalTableFixture.config();
    TableIdentifier ident = TableIdentifier.of("logs", "t");
    Catalog caller = mock(Catalog.class);
    BackendCatalogs catalogs = new BackendCatalogs(TestConfigs.proxyConfig());
    catalogs.seedCallerCatalog("lake", "Bearer tok", caller);
    CountRoutes routes =
        new CountRoutes(config.format(), catalogs, new TermIndex(config.format(), new Metrics()));
    byte[] body = "{\"column\":\"ip\",\"term\":\"x\"}".getBytes(StandardCharsets.UTF_8);

    // doThrow, not when(...).thenThrow: re-stubbing a method already stubbed to throw would
    // invoke it, and the first refusal would surface as the test's own failure
    doThrow(new ForbiddenException("no read on %s", ident)).when(caller).loadTable(ident);
    PlanRoutes.Result forbidden = routes.count("lake", "logs", "t", body, "Bearer tok");
    assertEquals(403, forbidden.status(), new String(forbidden.body(), StandardCharsets.UTF_8));

    doThrow(new NoSuchTableException("Table does not exist: %s", ident)).when(caller).loadTable(ident);
    PlanRoutes.Result missing = routes.count("lake", "logs", "t", body, "Bearer tok");
    assertEquals(404, missing.status(), new String(missing.body(), StandardCharsets.UTF_8));
  }

  /**
   * The prefix arrives raw from the path and the caller client is keyed by the decoded form, the
   * form the service client and the observed-snapshot map use. Under caller identity a Nessie
   * prefix like {@code main|wh} arrives as {@code main%7Cwh}; keyed raw it would miss the client
   * built for it and mint another. Only the seeded client can answer 403 here, so a 403 proves the
   * decode. Verified red with the raw segment passed through.
   */
  @Test
  void aCountDecodesThePrefixBeforeChoosingTheCallersClient() {
    BuildConfig config = LocalTableFixture.config();
    TableIdentifier ident = TableIdentifier.of("logs", "t");
    Catalog caller = mock(Catalog.class);
    BackendCatalogs catalogs = new BackendCatalogs(TestConfigs.proxyConfig());
    catalogs.seedCallerCatalog("main|wh", "Bearer tok", caller);
    doThrow(new ForbiddenException("no read on %s", ident)).when(caller).loadTable(ident);
    CountRoutes routes =
        new CountRoutes(config.format(), catalogs, new TermIndex(config.format(), new Metrics()));
    byte[] body = "{\"column\":\"ip\",\"term\":\"x\"}".getBytes(StandardCharsets.UTF_8);

    PlanRoutes.Result forbidden = routes.count("main%7Cwh", "logs", "t", body, "Bearer tok");
    assertEquals(403, forbidden.status(), new String(forbidden.body(), StandardCharsets.UTF_8));
  }
}
