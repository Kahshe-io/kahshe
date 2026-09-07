package io.kahshe;

import io.kahshe.indexer.TableSource;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tables from any Iceberg catalog, resolved the way Iceberg resolves one: an implementation class
 * name from {@code KAHSHE_CATALOG_IMPL} plus dotted properties from
 * {@code KAHSHE_CATALOG_PROPERTIES}, handed to {@link CatalogUtil#loadCatalog}. Glue, Hive, JDBC,
 * DynamoDB or a REST catalog with its own signer are all configuration rather than code.
 *
 * <p>For a process with no data plane only. The proxy IS a REST catalog — engines speak REST to
 * it and it forwards everything it does not serve — so it needs the REST client and gets
 * {@code BackendCatalogs} instead.
 *
 * <p>A prefix is a REST catalog's tenancy path segment and is not a routing key here: one catalog
 * serves every rule, and a rule's prefix stays part of its identity in alerts rather than
 * selecting a client. A multi-tenant deployment runs one watcher per catalog.
 */
final class CatalogSource implements TableSource, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogSource.class);

  private final Catalog catalog;
  private final String impl;

  private CatalogSource(Catalog catalog, String impl) {
    this.catalog = catalog;
    this.impl = impl;
  }

  /**
   * Builds the catalog {@code impl} names, or returns null when none is configured — the caller
   * then uses the REST client, which is the default and the only one the proxy can use.
   *
   * @param impl the {@code Catalog} implementation class, from {@code KAHSHE_CATALOG_IMPL}
   * @param properties its dotted configuration, from {@code KAHSHE_CATALOG_PROPERTIES}
   * @param warehouse the warehouse location, when the properties do not carry one
   */
  static CatalogSource fromEnv(String impl, Map<String, String> properties, String warehouse) {
    if (impl == null || impl.isBlank()) {
      return null;
    }
    Map<String, String> props = new LinkedHashMap<>(properties);
    if (!warehouse.isBlank()) {
      props.putIfAbsent(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
    }
    // manifests and manifest lists are immutable; caching their bytes is worth it on a scan pass
    // that walks the same metadata every poll.
    props.putIfAbsent(CatalogProperties.IO_MANIFEST_CACHE_ENABLED, "true");
    LOG.info("catalog: {} with {} propert(ies) — the rule prefix is part of a rule's identity "
        + "here, not a client selector", impl, props.size());
    // null hadoopConf: loadCatalog forwards only a non-null one to a Configurable catalog, and a
    // Hive or Glue deployment that needs one sets it through the properties.
    return new CatalogSource(CatalogUtil.loadCatalog(impl, "kahshe", props, null), impl);
  }

  @Override
  public Table load(String prefix, TableIdentifier ident) {
    return catalog.loadTable(ident);
  }

  /**
   * A no-op, and deliberately: nothing here is wrapped in a {@code CachingCatalog}, so every
   * {@link #load} already reads the catalog. A watcher reads a table once per poll and has nothing
   * to gain from caching and something to lose — a stale view means scanning files that are not
   * there, or missing files that are.
   */
  @Override
  public void invalidate(String prefix, TableIdentifier ident) {}

  @Override
  public void close() {
    if (catalog instanceof AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (Exception e) {
        LOG.warn("closing the {} catalog failed", impl, e);
      }
    }
  }
}
