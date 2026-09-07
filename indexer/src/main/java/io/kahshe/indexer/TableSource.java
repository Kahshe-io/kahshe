package io.kahshe.indexer;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Where the indexer gets its tables: the one seam between building and whatever catalog the
 * deployment fronts. The proxy's catalog clients implement it; a catalog embedding the indexer
 * implements it over its own {@code Catalog}. A prefix is the REST catalog's tenancy path segment,
 * empty where a catalog has none.
 */
public interface TableSource {
  /** The current table, from the catalog behind {@code prefix}. */
  Table load(String prefix, TableIdentifier ident);

  /** Forgets any cached view of the table, so the next {@link #load} reads the catalog again. */
  void invalidate(String prefix, TableIdentifier ident);
}
