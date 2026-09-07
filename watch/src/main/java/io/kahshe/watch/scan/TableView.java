package io.kahshe.watch.scan;

import io.kahshe.analysis.ValueKind;
import java.util.Map;

/**
 * A table, in the only terms a scanner needs it: a name to say in a log line, and what its
 * columns ARE.
 *
 * <p>This is the port. Everything above it — the rules, the conditions, the windows, the alerts —
 * is written against this and not against Iceberg, so the work of serving a second table format is
 * one reader and this record, rather than the evaluation path.
 *
 * <p>Only TOP-LEVEL columns appear. A rule naming a nested path finds nothing here and is counted
 * as uncovered by discovery, rather than being projected and then failing on every row.
 *
 * @param name the table's name, for logs
 * @param columns each top-level column's name and kind
 */
public record TableView(String name, Map<String, ValueKind> columns) {

  public TableView {
    columns = Map.copyOf(columns);
  }

  /** Whether the table has this column at all — the only schema question a scanner asks. */
  public boolean has(String column) {
    return columns.containsKey(column);
  }

  /** This column's kind, or {@link ValueKind#OTHER} when the table does not have it. */
  public ValueKind kind(String column) {
    return columns.getOrDefault(column, ValueKind.OTHER);
  }
}
