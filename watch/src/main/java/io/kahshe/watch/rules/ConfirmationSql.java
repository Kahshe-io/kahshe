package io.kahshe.watch.rules;

import io.kahshe.analysis.ValueKind;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Builds the alert's confirmation SQL (Trino dialect, snapshot-scoped). kahshe never executes
 * SQL — the operator's own engine runs this, under its own authorization, to verify an alert.
 *
 * <p>Both token and contains evidence emit {@code position(...) &gt; 0}: for tokens this is a
 * superset pre-filter — position() matches substrings beyond token boundaries — so the query can
 * over-return but never under-return. {@code LIKE} with an explicit {@code ESCAPE} carries the
 * prefix and suffix operators; only the {@code re} operator emits {@code regexp_like}, because
 * only there did the rule ask for a regular expression.
 *
 * <p>The condition is the rule's tree, so a {@code detection} rule's {@code and}/{@code or}/
 * {@code not} over selections reaches the engine as written. This is one of the tree's two
 * interpreters; {@link Conditions} is the other and carries what the two must agree on —
 * including why a negated term here is wrapped in {@code COALESCE(…, false)} rather than left a
 * bare {@code NOT}.
 */
public final class ConfirmationSql {
  private ConfirmationSql() {}

  /**
   * The single-column form: literals over one column, joined by the rule's condition, each as a
   * {@code position()} probe.
   */
  public static String build(
      String catalog,
      String namespace,
      String table,
      long snapshotId,
      String filePath,
      String column,
      List<String> literals,
      boolean allOf) {
    List<WatchRule.Field> fields = new ArrayList<>();
    for (String literal : literals) {
      fields.add(new WatchRule.Field(column, WatchRule.Op.CONTAINS, List.of(literal)));
    }
    return build(catalog, namespace, table, snapshotId, filePath, fields, Set.of(),
        WatchRule.Expr.flat(
            allOf ? WatchRule.Condition.ALL_OF : WatchRule.Condition.ANY_OF, fields.size()));
  }

  /**
   * The whole condition across columns: one predicate per field, joined by the rule's condition,
   * with a field's own values OR'ed inside it. {@code numericColumns} names the columns whose
   * {@code equals} literals are numbers rather than quoted text — the scan resolves that from the
   * table's schema, so the SQL compares an int column to an int.
   *
   * <p>Every field's predicate is emitted, not only the ones that matched: the SQL must ask the
   * rule as written, and a condition missing half its conjuncts would confirm something else.
   */
  public static String build(
      String catalog,
      String namespace,
      String table,
      long snapshotId,
      String filePath,
      List<WatchRule.Field> fields,
      Set<String> numericColumns,
      WatchRule.Expr expr) {
    StringBuilder sb = new StringBuilder("SELECT * FROM ");
    sb.append(catalog).append('.').append(ident(namespace)).append('.').append(ident(table));
    sb.append(" FOR VERSION AS OF ").append(snapshotId);
    sb.append(" WHERE \"$path\" = ").append(literal(filePath));
    sb.append(" AND (").append(condition(expr, fields, numericColumns, true)).append(')');
    return sb.toString();
  }

  /**
   * The condition tree: leaves are field predicates, compounds parenthesised — except the top
   * level, whose parentheses the WHERE clause already supplies.
   */
  private static String condition(
      WatchRule.Expr expr, List<WatchRule.Field> fields, Set<String> numericColumns, boolean top) {
    if (expr instanceof WatchRule.Expr.FieldRef ref) {
      WatchRule.Field field = fields.get(ref.index());
      return predicate(field, numericColumns.contains(field.column()));
    }
    if (expr instanceof WatchRule.Expr.Not not) {
      // COALESCE, not a bare NOT: see Conditions. A null column makes the inner predicate NULL,
      // and NOT NULL is NULL, which drops a row Conditions.eval counted.
      return "NOT COALESCE(" + condition(not.term(), fields, numericColumns, false) + ", false)";
    }
    List<WatchRule.Expr> terms;
    String join;
    if (expr instanceof WatchRule.Expr.And and) {
      terms = and.terms();
      join = " AND ";
    } else {
      terms = ((WatchRule.Expr.Or) expr).terms();
      join = " OR ";
    }
    StringBuilder sb = new StringBuilder(top ? "" : "(");
    for (int i = 0; i < terms.size(); i++) {
      if (i > 0) {
        sb.append(join);
      }
      sb.append(condition(terms.get(i), fields, numericColumns, false));
    }
    return top ? sb.toString() : sb.append(')').toString();
  }

  /** One field: its values OR'ed, parenthesised when there is more than one. */
  private static String predicate(WatchRule.Field field, boolean numeric) {
    StringBuilder sb = new StringBuilder();
    List<String> values = field.values();
    if (values.size() > 1) {
      sb.append('(');
    }
    for (int i = 0; i < values.size(); i++) {
      if (i > 0) {
        sb.append(" OR ");
      }
      sb.append(term(field.column(), field.op(), values.get(i), numeric));
    }
    if (values.size() > 1) {
      sb.append(')');
    }
    return sb.toString();
  }

  private static String term(String column, WatchRule.Op op, String value, boolean numeric) {
    String lowered = value.toLowerCase(Locale.ROOT);
    return switch (op) {
      // A token is confirmed as a substring: it over-returns, never under-returns.
      case MATCH, CONTAINS ->
          "position(" + literal(lowered) + " IN lower(" + ident(column) + ")) > 0";
      // Whole value, so no lower(): equality is exact on a string and numeric on a number, which
      // is what the scan compared.
      case EQUALS -> ident(column) + " = " + (numeric ? value : literal(value));
      // A numeric column compares as a number, as the scan did: it canonicalised "07045" to 7045,
      // and lower('07045') would match nothing. Otherwise both sides are folded as the scan folded
      // them; the CAST keeps the expression valid whatever the column's type, and costs nothing on
      // a query already pinned to one file.
      case EQUALS_IGNORE_CASE -> numeric
          ? ident(column) + " = " + value
          : "lower(CAST(" + ident(column) + " AS varchar)) = " + literal(lowered);
      case STARTS_WITH ->
          "lower(" + ident(column) + ") LIKE " + literal(like(lowered) + "%") + " ESCAPE '\\'";
      case ENDS_WITH ->
          "lower(" + ident(column) + ") LIKE " + literal("%" + like(lowered)) + " ESCAPE '\\'";
      // The rule asked for a regular expression, so the SQL asks for one. Case-sensitive and
      // unanchored, as the scan's find() is.
      case RE -> "regexp_like(" + ident(column) + ", " + literal(value) + ")";
      // A numeric column compares directly; anything else is cast, and TRY_CAST rather than CAST
      // so a row whose text is not a number is excluded instead of failing the query — which is
      // what the scan does with it.
      case GT, GTE, LT, LTE -> numericOperand(column, numeric) + " " + sqlOp(op) + " " + value;
    };
  }

  private static String numericOperand(String column, boolean numeric) {
    return numeric ? ident(column) : "TRY_CAST(" + ident(column) + " AS DOUBLE)";
  }

  private static String sqlOp(WatchRule.Op op) {
    return switch (op) {
      case GT -> ">";
      case GTE -> ">=";
      case LT -> "<";
      case LTE -> "<=";
      default -> throw new IllegalArgumentException("not a comparison: " + op);
    };
  }

  /**
   * A window rule's confirmation: the exact grouped count over the window the alert names, on the
   * whole table rather than one file — a window's rows are spread across files, which is the
   * reason the alert needed state at all. The time bound is what lets Iceberg's own min/max
   * pruning apply: on a time-ordered or time-partitioned table the engine reads the window's files
   * and no others.
   *
   * @param startMs the window's first event time, inclusive
   * @param endMs its last, inclusive
   */
  public static String window(
      String catalog,
      String namespace,
      String table,
      long snapshotId,
      WatchRule rule,
      Set<String> numericColumns,
      ValueKind tsKind,
      long startMs,
      long endMs) {
    WatchRule.Window window = rule.window();
    List<String> groupBy = window.groupBy();
    StringBuilder sb = new StringBuilder("SELECT ");
    for (String column : groupBy) {
      sb.append(ident(column)).append(", ");
    }
    sb.append("count(*) AS n FROM ");
    sb.append(catalog).append('.').append(ident(namespace)).append('.').append(ident(table));
    sb.append(" FOR VERSION AS OF ").append(snapshotId);
    sb.append(" WHERE ").append(ident(window.tsColumn()));
    sb.append(" BETWEEN ").append(timestamp(startMs, tsKind))
        .append(" AND ").append(timestamp(endMs, tsKind));
    sb.append(" AND (").append(condition(rule.expr(), rule.where(), numericColumns, true)).append(')');
    for (int i = 0; i < groupBy.size(); i++) {
      sb.append(i == 0 ? " GROUP BY " : ", ").append(ident(groupBy.get(i)));
    }
    sb.append(" HAVING count(*) >= ").append(window.count());
    return sb.toString();
  }

  private static final DateTimeFormatter UTC_LITERAL =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);
  private static final DateTimeFormatter UTC_DATE =
      DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);

  /**
   * A window bound, written for the KIND of the column it will be compared against — exactly the
   * kinds {@link WindowScanner#epochMillis} accepts: an instant literal marked UTC for
   * {@code timestamptz}, a wall-clock literal for {@code timestamp}, a date literal for
   * {@code date}, the raw millis for an integral epoch column, and ISO-8601 text for a string one.
   *
   * <p>Two constraints decide this. Each form must compare without consulting the reader's session
   * zone, or the same confirmation returns different rows for different operators — and one that
   * returns none reads as a false positive rather than as a broken query. No unit test can catch
   * that, because a unit test has no session zone. And each leaves the column BARE: wrapping it in
   * {@code AT TIME ZONE} answers correctly but hides it from Iceberg's min/max pruning, which is
   * what keeps the query to the window's own files.
   *
   * <p>The string case is the weakest, and the one shape here not verified against an engine: it
   * assumes the stored text is the ISO-8601 UTC form {@code Instant.parse} reads, the same
   * assumption {@code epochMillis} makes, but it is bracketed lexicographically here, so a
   * zone-less spelling can disagree.
   */
  private static String timestamp(long epochMs, ValueKind tsKind) {
    Instant instant = Instant.ofEpochMilli(epochMs);
    return switch (tsKind) {
      case TIMESTAMPTZ -> "TIMESTAMP '" + UTC_LITERAL.format(instant) + " UTC'";
      case TIMESTAMP -> "TIMESTAMP '" + UTC_LITERAL.format(instant) + "'";
      case DATE -> "DATE '" + UTC_DATE.format(instant) + "'";
      case INTEGRAL -> Long.toString(epochMs);
      default -> literal(instant.toString());
    };
  }

  static String ident(String name) {
    return '"' + name.replace("\"", "\"\"") + '"';
  }

  static String literal(String value) {
    return '\'' + value.replace("'", "''") + '\'';
  }

  /** A literal inside a LIKE pattern: the two wildcards and the escape itself are escaped. */
  static String like(String value) {
    StringBuilder sb = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c == '\\' || c == '%' || c == '_') {
        sb.append('\\');
      }
      sb.append(c);
    }
    return sb.toString();
  }
}
