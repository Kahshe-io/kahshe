package io.kahshe.proxy.catalog;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.junit.jupiter.api.Test;

/**
 * Turning the table cache off has to mean NO cache, not a short one.
 *
 * <p>{@code reconcileToObserved} closes the staleness window by planning the snapshot this proxy
 * forwarded, and it is per-process. With more than one replica a client can loadTable through A —
 * where the observation lands — and plan through B, which holds neither that observation nor A's
 * invalidation and answers from a view of unknown age. A replica that caches nothing cannot hold a
 * stale view at all, which makes replica count irrelevant rather than load-bearing, and costs one
 * metadata fetch per plan.
 *
 * <p>So the disabled setting must remove the wrapper rather than shorten it. A zero-length TTL
 * would still be a cache — a request arriving inside the same millisecond would hit it, which is
 * rare enough to be untestable and frequent enough to be real under load.
 */
class TableCacheTest {

  private static Catalog uninitialised() {
    // Never invoked, only wrapped: CachingCatalog.wrap takes a Catalog and this is one.
    return new RESTCatalog();
  }

  /**
   * Zero means the catalog is handed back untouched.
   *
   * <p>Identity is the assertion, deliberately. Anything weaker — "it is not a CachingCatalog" —
   * would pass against a wrapper that merely reports itself differently.
   *
   * <p>Verified by breaking it: wrapping unconditionally fails the first assertion.
   */
  @Test
  void aZeroTtlLeavesTheCatalogUnwrapped() {
    Catalog raw = uninitialised();
    assertSame(raw, BackendCatalogs.maybeCache(raw, 0), "0 must mean no cache, not a brief one");
    assertSame(raw, BackendCatalogs.maybeCache(raw, -1), "a negative TTL is not a cache either");
  }

  /** And the default still caches, or this would pass against the feature being deleted. */
  @Test
  void aPositiveTtlStillWrapsInACache() {
    Catalog raw = uninitialised();
    Catalog wrapped = BackendCatalogs.maybeCache(raw, 10_000);
    assertNotSame(raw, wrapped, "a positive TTL must still cache");
    assertTrue(wrapped instanceof CachingCatalog, "and the wrapper must be the cache: " + wrapped);
  }
}
