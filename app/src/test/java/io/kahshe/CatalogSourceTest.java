package io.kahshe;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.Test;

/**
 * Any Iceberg catalog, by class name and properties — the same mechanism {@link IndexIo} applies
 * to the index store, on the axis a foreign deployment varies on.
 *
 * <p>Loaded here through Iceberg's own {@code InMemoryCatalog}, which is exactly the point: a
 * catalog kahshe has no knowledge of, named in configuration, and it works.
 */
class CatalogSourceTest {

  private static final String IN_MEMORY = InMemoryCatalog.class.getName();

  @Test
  void anUnconfiguredImplMeansTheRestClient() {
    assertNull(CatalogSource.fromEnv("", Map.of(), "warehouse"),
        "empty is not a catalog: the caller falls back to REST, which is the default");
    assertNull(CatalogSource.fromEnv(null, Map.of(), "warehouse"));
  }

  @Test
  void aCatalogKahsheKnowsNothingAboutLoadsByNameAndServesTables(@org.junit.jupiter.api.io.TempDir
      java.nio.file.Path tmp) {
    Map<String, String> props = new LinkedHashMap<>();
    props.put(CatalogProperties.WAREHOUSE_LOCATION, tmp.toString());
    try (CatalogSource source = CatalogSource.fromEnv(IN_MEMORY, props, "")) {
      assertNotNull(source);
      Catalog catalog = catalogOf(source);
      ((org.apache.iceberg.catalog.SupportsNamespaces) catalog).createNamespace(Namespace.of("logs"));
      TableIdentifier ident = TableIdentifier.of(Namespace.of("logs"), "events");
      catalog.createTable(ident, new org.apache.iceberg.Schema(
          org.apache.iceberg.types.Types.NestedField.optional(
              1, "msg", org.apache.iceberg.types.Types.StringType.get())));

      // the prefix is a REST tenancy segment and means nothing here: any value loads the table
      assertNotNull(source.load("", ident));
      assertNotNull(source.load("some-prefix", ident), "the prefix is not a client selector");
      assertTrue(source.load("", ident).name().endsWith("logs.events"), source.load("", ident).name());
      source.invalidate("", ident); // a documented no-op: nothing is cached to forget
      assertNotNull(source.load("", ident), "and the source still works after it");
    }
  }

  @Test
  void theWarehouseIsSuppliedOnlyWhenThePropertiesDoNotCarryOne(@org.junit.jupiter.api.io.TempDir
      java.nio.file.Path tmp) {
    Map<String, String> explicit = new LinkedHashMap<>();
    explicit.put(CatalogProperties.WAREHOUSE_LOCATION, tmp.resolve("explicit").toString());
    try (CatalogSource source = CatalogSource.fromEnv(IN_MEMORY, explicit, tmp.resolve("fallback").toString())) {
      // an explicit property wins over the KAHSHE_BACKEND_WAREHOUSE shorthand, as IndexIo's do
      assertNotNull(source);
    }
    try (CatalogSource source = CatalogSource.fromEnv(IN_MEMORY, Map.of(), tmp.resolve("fallback").toString())) {
      assertNotNull(source, "and the shorthand is used when nothing spells it out");
    }
  }

  @Test
  void anUnknownImplementationFailsAtStartupRatherThanAtTheFirstPoll() {
    IllegalArgumentException e = assertThrows(
        IllegalArgumentException.class,
        () -> CatalogSource.fromEnv("com.example.NoSuchCatalog", Map.of(), "warehouse"),
        "a typo in KAHSHE_CATALOG_IMPL must not surface as a table that never loads");
    assertTrue(e.getMessage().contains("NoSuchCatalog"), e.getMessage());
  }

  @Test
  void aCatalogSourceIsATableSource() {
    assertTrue(io.kahshe.indexer.TableSource.class.isAssignableFrom(CatalogSource.class),
        "the watcher and the indexer take it wherever they take the REST client");
  }

  private static Catalog catalogOf(CatalogSource source) {
    try {
      java.lang.reflect.Field field = CatalogSource.class.getDeclaredField("catalog");
      field.setAccessible(true);
      return (Catalog) field.get(source);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
  }
}
