package io.kahshe.proxy;

import io.kahshe.common.Metrics;
import com.sun.net.httpserver.HttpHandler;
import io.kahshe.indexer.maintain.IndexerService;
import java.util.Map;
import io.kahshe.proxy.catalog.BackendCatalogs;
import io.kahshe.proxy.http.KahsheHandler;

/**
 * Wiring for tests of the HTTP surface.
 *
 * <p>{@code Kahshe.Config} is a wide positional record, so a test that constructed one literally
 * would break every time a knob was added and would bury the two values it actually cares about in
 * forty that it does not. This copies an existing config and overrides fields BY NAME, which
 * survives new components and states the intent at the call site.
 */
public final class TestConfigs {
  private TestConfigs() {}

  /** See {@link io.kahshe.common.Records#with(java.lang.Record, Map)}; kept for the root tests. */
  public static <T extends java.lang.Record> T with(T base, Map<String, Object> overrides) {
    return io.kahshe.common.Records.with(base, overrides);
  }

  /** The serving side's record, as the app's {@code fromEnv} would build it for a test. */
  public static ProxyConfig proxyConfig() {
    return new ProxyConfig(
        "http://localhost:0", "", "", true, 0, 1_000, 1 << 20, "service", "",
        false, // serveDeleteBearing: the suite exercises the guard, not the escape hatch
        10_000L, // tableCacheTtlMs: the production default; TableCacheTest varies it
        "strip", // planStats: the default; PlanStatsOptionTest varies it
        ""); // backendCa: the JVM default trust store, as an unconfigured deployment uses
  }

  public static ProxyConfig proxy(String backendBase, int maxBodyBytes) {
    return with(
        proxyConfig(),
        Map.of("backendBase", backendBase, "maxBodyBytes", maxBodyBytes));
  }

  /**
   * The real dispatcher, wired the way {@code Kahshe} wires it, with the indexer off, on the
   * {@code Metrics} and the catalogs the caller holds — so a test can assert what the proxy
   * counted and what it observed.
   */
  public static HttpHandler handler(ProxyConfig config, Metrics metrics, BackendCatalogs catalogs) {
    io.kahshe.indexer.BuildConfig build = io.kahshe.indexer.LocalTableFixture.config();
    return new KahsheHandler(
        config, build.format(), metrics, catalogs,
        new IndexerService(catalogs, metrics, false, build));
  }
}
