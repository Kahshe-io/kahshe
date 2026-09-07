package io.kahshe.format;

import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.expressions.Expressions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Which of a table's data files the index covers: the table property {@code kahshe.index.scope}
 * holds an Iceberg expression, in the JSON form Iceberg itself serialises, that is handed to
 * {@code newScan().filter(...)} so the build reads only what it admits.
 *
 * <p>A partial index cannot produce a wrong answer, only less pruning — a file the index does not
 * know about is kept — so coverage is a cost/benefit dial with no correctness cliff. A malformed
 * expression therefore fails the build: defaulting to everything or to nothing would be safe in
 * that sense and silently wrong operationally.
 */
public final class IndexScope {

  private static final Logger LOG = LoggerFactory.getLogger(IndexScope.class);

  /** Table property naming the expression. Absent means the whole table. */
  public static final String PROPERTY = "kahshe.index.scope";

  private IndexScope() {}

  /**
   * The scan filter for {@code table}, or {@link Expressions#alwaysTrue()} when the table declares
   * no scope.
   *
   * @throws IllegalArgumentException if the property is present but not a valid expression
   */
  public static Expression of(Table table) {
    String json = table.properties().getOrDefault(PROPERTY, "").trim();
    if (json.isEmpty()) {
      return Expressions.alwaysTrue();
    }
    Expression expression;
    try {
      expression = ExpressionParser.fromJson(json, table.schema());
    } catch (RuntimeException e) {
      // Loud, not lenient. Falling back to the whole table would silently cost hours on a large
      // one; falling back to nothing would silently stop pruning. Neither is a wrong answer, so
      // neither would be noticed.
      throw new IllegalArgumentException(
          PROPERTY + " is not a valid Iceberg expression: " + json, e);
    }
    LOG.info("index scope from {}: {}", PROPERTY, expression);
    return expression;
  }

  /** Whether {@code table} narrows its index coverage at all; recorded in the metadata. */
  public static boolean isNarrowed(Table table) {
    return !table.properties().getOrDefault(PROPERTY, "").trim().isEmpty();
  }
}
