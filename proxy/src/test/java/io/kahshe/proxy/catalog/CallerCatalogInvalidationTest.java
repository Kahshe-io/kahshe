package io.kahshe.proxy.catalog;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;
import io.kahshe.proxy.TestConfigs;

/**
 * A commit seen in the passthrough must invalidate the caller-identity catalogs too.
 *
 * <p>Each caller catalog wraps its own {@code CachingCatalog}, and under
 * {@code KAHSHE_PLANNING_IDENTITY=caller} the plan is built from it. An {@code invalidate} that
 * touched only the service catalog for the prefix would let a caller's plan be built from a
 * pre-commit view for the whole TTL.
 */
class CallerCatalogInvalidationTest {
  @Test
  void aCommitInvalidatesEveryCallerCatalogsViewOfTheTable() {
    BackendCatalogs catalogs = new BackendCatalogs(TestConfigs.proxyConfig());
    Catalog callerA = mock(Catalog.class);
    Catalog callerB = mock(Catalog.class);
    catalogs.seedCallerCatalog("lake", "Bearer a", callerA);
    catalogs.seedCallerCatalog("lake", "Bearer b", callerB);
    TableIdentifier events = TableIdentifier.of("logs", "events");
    TableIdentifier other = TableIdentifier.of("logs", "other");

    catalogs.invalidate("lake", events);

    verify(callerA).invalidateTable(events);
    verify(callerB).invalidateTable(events);
    verify(callerA, never()).invalidateTable(other);
  }
}
