package io.kahshe.format;

import org.apache.iceberg.Table;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.Parquet;

/**
 * The name mapping a data file without field ids is read under, and why a file with neither is
 * refused.
 *
 * <p>Iceberg resolves a Parquet file's columns by the field ids in the file; failing that by the
 * table's {@code schema.name-mapping.default}; failing that by column POSITION. A table built by
 * {@code add_files} or a Hive {@code migrate} keeps its original Parquet, which carries no ids, so
 * it relies on the mapping — and a reader that skips it falls through to positions, which equal
 * field ids only for a schema that has never dropped, reordered, or added a column out of order.
 * Where they differ the build publishes one column's tokens under another's id, the index covers
 * every file, and a probe prunes exactly the files that match. A wrong answer with no error is the
 * outcome the format exists to prevent, so the caller refuses a file with neither ids nor a
 * mapping rather than let the fallback run.
 */
public final class DataFileIds {

  /** Iceberg's own property; the name is the spec's, not kahshe's. */
  public static final String NAME_MAPPING = "schema.name-mapping.default";

  private DataFileIds() {}

  /** The table's name mapping, or null when it declares none. */
  public static NameMapping mappingOf(Table table) {
    String json = table.properties() == null ? null : table.properties().get(NAME_MAPPING);
    if (json == null || json.isBlank()) {
      return null;
    }
    try {
      return NameMappingParser.fromJson(json);
    } catch (RuntimeException e) {
      throw new IllegalStateException(
          "the table's " + NAME_MAPPING + " could not be parsed, so a data file without field ids "
              + "cannot be resolved by name and would be read by COLUMN POSITION instead — which "
              + "silently indexes the wrong column when a schema has ever dropped or reordered "
              + "one. Fix the property, or rewrite the files with field ids", e);
    }
  }

  /**
   * Applies the mapping to a read that needs it.
   *
   * <p>Passing it is harmless when the file already carries ids: Iceberg prefers the file's own.
   */
  public static <T> Parquet.ReadBuilder withMapping(Parquet.ReadBuilder builder, NameMapping mapping) {
    return mapping == null ? builder : builder.withNameMapping(mapping);
  }
}
