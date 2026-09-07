package io.kahshe.proxy.catalog;

import io.kahshe.common.Hashing;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.kahshe.proxy.ProxyConfig;

/**
 * Internal REST catalog clients pointed directly at the backing catalog, one per path prefix.
 *
 * <p>These clients stay in client planning mode (the default), so planning executes locally in the
 * proxy from manifests. The prefix in plan-endpoint paths is treated as the warehouse name, which
 * holds for Polaris (prefix == catalog name) but not for Nessie, whose prefix encodes branch and
 * warehouse ("main|warehouse"); KAHSHE_BACKEND_WAREHOUSE overrides the guess there.
 */
public final class BackendCatalogs implements io.kahshe.indexer.TableSource {
  private static final Logger LOG = LoggerFactory.getLogger(BackendCatalogs.class);

  /**
   * Wraps {@code catalog} in a table cache when {@code ttlMs} is positive
   * ({@code KAHSHE_TABLE_CACHE_TTL_MS}, default 10 s). The TTL is a cost control, not the
   * correctness mechanism: {@link #reconcileToObserved} and exact invalidation catch staleness,
   * and only a table this replica never forwarded, or a mutating body it could not parse
   * ({@code kahshe_invalidation_parse_failures_total}), waits on the timer.
   *
   * <p>Zero means no cache at all, not a zero-length one: a replica that caches nothing cannot
   * hold a stale view, which is what a multi-replica deployment needs.
   */
  static Catalog maybeCache(Catalog catalog, long ttlMs) {
    return ttlMs > 0 ? CachingCatalog.wrap(catalog, ttlMs) : catalog;
  }

  private final ProxyConfig config;
  private final Map<String, Catalog> catalogs = new ConcurrentHashMap<>();
  /**
   * Caller-scoped catalogs, keyed by token hash and prefix, closed when evicted. Each entry is a
   * fully initialized {@code RESTCatalog} — a pooled HTTP client and a token-refresh executor — and
   * under {@code KAHSHE_PLANNING_IDENTITY=caller} every token rotation mints one and evicts
   * another, so an entry left unclosed leaks its thread and its sockets for the life of the process.
   */
  private final io.kahshe.common.BoundedCache<String, Catalog> callerCatalogs =
      new io.kahshe.common.BoundedCache<>(64, BackendCatalogs::closeQuietly);

  private static void closeQuietly(Catalog catalog) {
    if (catalog instanceof AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (Exception e) {
        // an evicted catalog that will not close is a leak, not a request failure: say so and
        // carry on rather than failing the caller whose request happened to trigger the eviction
        LOG.warn("failed to close an evicted caller catalog", e);
      }
    }
  }

  public BackendCatalogs(ProxyConfig config) {
    this.config = config;
  }

  public Catalog forPrefix(String prefix) {
    return catalogs.computeIfAbsent(
        prefix, p -> maybeCache(create(p), config.tableCacheTtlMs()));
  }

  /** Per-token lock for {@link #forCaller}, so one token's initialization blocks only itself. */
  private final io.kahshe.common.SingleFlight<String> callerFlight =
      new io.kahshe.common.SingleFlight<>();

  /**
   * A catalog client authenticated as the caller (static bearer token), so the backend's own
   * authorization — including vended-credential scoping — applies to planning reads. Built at most
   * once per token even under a burst.
   *
   * <p>Construction performs a network-bound {@code RESTCatalog.initialize()}, so it must not run
   * under {@code BoundedCache.computeIfAbsent}, which is {@code synchronized} on the whole cache;
   * {@link io.kahshe.common.SingleFlight} locks per key instead, so two different tokens initialize
   * concurrently while two requests bearing the same token share one initialization.
   */
  public Catalog forCaller(String prefix, String bearerToken) {
    String token = bearerToken.startsWith("Bearer ") ? bearerToken.substring(7) : bearerToken;
    String key = Hashing.sha256Base64(token) + "|" + prefix;
    Catalog cached = callerCatalogs.get(key);
    if (cached != null) {
      return cached;
    }
    return callerFlight.load(
        key,
        () -> callerCatalogs.get(key),
        () -> {
          LOG.info("initializing caller-identity catalog client for prefix '{}'", prefix);
          RESTCatalog catalog = new RESTCatalog();
          catalog.setConf(new Configuration());
          Map<String, String> props = new ConcurrentHashMap<>();
          props.put(CatalogProperties.URI, config.backendBase());
          props.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseFor(prefix));
          props.put(CatalogProperties.IO_MANIFEST_CACHE_ENABLED, "true");
          props.put(OAuth2Properties.TOKEN, token);
          catalog.initialize("kahshe-caller-" + prefix, props);
          Catalog wrapped = maybeCache(catalog, config.tableCacheTtlMs());
          callerCatalogs.put(key, wrapped);
          return wrapped;
        });
  }

  private String warehouseFor(String prefix) {
    return config.backendWarehouse().isBlank() ? prefix : config.backendWarehouse();
  }


  /**
   * The newest snapshot this proxy has forwarded to a client for each table. Planning a view older
   * than the one it handed out is the one way the client and the proxy can disagree, and it is
   * silent: the plan response names no snapshot, so nothing downstream can tell.
   *
   * <p>Bounded and per-process. A replica that never forwarded a given table has no opinion about
   * it and falls back to the cached view, so across several replicas this narrows the window rather
   * than closing it.
   */
  private final io.kahshe.common.BoundedCache<String, Long> observedSnapshots =
      new io.kahshe.common.BoundedCache<>(4096);

  /** One construction site for the key, so the writer and the reader cannot drift apart. */
  private static String observedKey(String prefix, TableIdentifier ident) {
    return prefix + "|" + ident;
  }

  /** Records the snapshot a forwarded loadTable told a client about. */
  public void noteObserved(String prefix, TableIdentifier ident, long snapshotId) {
    if (snapshotId > 0) {
      observedSnapshots.put(observedKey(prefix, ident), snapshotId);
    }
  }

  /** The snapshot last forwarded for this table, or null if this replica has forwarded none. */
  public Long observedSnapshot(String prefix, TableIdentifier ident) {
    return observedSnapshots.get(observedKey(prefix, ident));
  }

  /**
   * Whether {@code table} is an older view than {@code observed}. Membership, not comparison:
   * snapshot ids are random longs, so "older" cannot be read off the number. A history containing
   * the observed snapshot is at or ahead of it; only being behind is a failure.
   */
  static boolean predatesObserved(Table table, Long observed) {
    return observed != null && table.snapshot(observed) == null;
  }

  /**
   * Reloads {@code ident} if the cached view predates what this proxy has forwarded. In the
   * ordinary case the observed snapshot is the cached one, so this is a map lookup and a hit on a
   * cache the caller was about to read anyway.
   */
  public boolean reconcileToObserved(Catalog catalog, String prefix, TableIdentifier ident) {
    Long observed = observedSnapshots.get(observedKey(prefix, ident));
    if (observed == null) {
      return false;
    }
    try {
      if (!predatesObserved(catalog.loadTable(ident), observed)) {
        return false;
      }
      LOG.info(
          "cached view of {} predates snapshot {}, which this proxy forwarded to a client; "
              + "reloading before planning rather than serving the older view",
          ident, observed);
      // The catalog that will plan is invalidated directly as well: under caller identity it is
      // not the prefix catalog, and a reconcile that reloads the wrong view reloads nothing.
      catalog.invalidateTable(ident);
      invalidate(prefix, ident);
      return true;
    } catch (RuntimeException e) {
      // Cannot tell: leave the cache alone, and planning proceeds from the cached view.
      LOG.debug("could not reconcile {} against the forwarded snapshot", ident, e);
      return false;
    }
  }

  /** Test seam: installs a caller-scoped catalog under {@code key} without a network init. */
  public void seedCallerCatalog(String key, Catalog catalog) {
    callerCatalogs.put(key, catalog);
  }

  /** {@link io.kahshe.indexer.TableSource}: the current table from the catalog behind the prefix. */
  @Override
  public org.apache.iceberg.Table load(String prefix, TableIdentifier ident) {
    return forPrefix(prefix).loadTable(ident);
  }

  /**
   * Exact invalidation, driven by commit responses observed in the passthrough. It must reach the
   * caller-identity catalogs too, since each wraps its own {@code CachingCatalog} and under
   * {@code KAHSHE_PLANNING_IDENTITY=caller} the plan is built from the caller's. Every caller
   * catalog is told regardless of prefix: the keys carry the token hash, not a prefix to filter on,
   * and invalidating a table a catalog never loaded is a no-op.
   */
  @Override
  public void invalidate(String prefix, TableIdentifier ident) {
    Catalog catalog = catalogs.get(prefix);
    if (catalog != null) {
      catalog.invalidateTable(ident);
      LOG.info("invalidated cached table {} in prefix '{}'", ident, prefix);
    }
    for (Catalog caller : callerCatalogs.values()) {
      caller.invalidateTable(ident);
    }
  }

  private RESTCatalog create(String prefix) {
    LOG.info("initializing backend catalog client for prefix '{}'", prefix);
    RESTCatalog catalog = new RESTCatalog();
    catalog.setConf(new Configuration());
    Map<String, String> props = new ConcurrentHashMap<>();
    props.put(CatalogProperties.URI, config.backendBase());
    props.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseFor(prefix));
    // manifests and manifest lists are immutable; cache their bytes across plans
    props.put(CatalogProperties.IO_MANIFEST_CACHE_ENABLED, "true");
    // A backend behind a private CA: Iceberg's only seam for this is a configurer class named
    // by property, which then reads our CA path out of the same map.
    if (!config.backendCa().isBlank()) {
      props.put(BackendTls.CONFIGURER_PROPERTY, BackendTls.class.getName());
      props.put(BackendTls.CA_PROPERTY, config.backendCa());
    }
    if (!config.credential().isBlank()) {
      props.put(OAuth2Properties.CREDENTIAL, config.credential());
      props.put(OAuth2Properties.OAUTH2_SERVER_URI, config.oauthTokenUri());
      props.put(OAuth2Properties.SCOPE, config.scope());
    }
    catalog.initialize("kahshe-" + prefix, props);
    return catalog;
  }
}
