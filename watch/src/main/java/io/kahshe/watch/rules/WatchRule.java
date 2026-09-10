package io.kahshe.watch.rules;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.List;
import java.util.Locale;

/**
 * One validated watch rule: a table, a list of field predicates over its columns, and a condition
 * joining them. The watcher evaluates rules prospectively, against data files as they arrive; a
 * rule of the shape {@link #ridesIndex} admits, less {@code contains} and {@code min_count}, can
 * also be asked over every file the table already holds by {@code kahshe hunt}, from the term
 * index alone ({@code io.kahshe.watch.scan.HuntPass}).
 *
 * <p>A rule is a list of {@link Field}s because a detection is a conjunction across columns, not a
 * predicate on one. The single-column YAML form is the case where every field names the same
 * column: {@code match: [a, b]} parses to one MATCH field per token, which {@link #match()} and
 * {@link #contains()} flatten back for the index-riding evaluator.
 *
 * <p>{@link #expr} is the condition as a tree over the fields, and it is the truth: the
 * {@code detection} form parses to an arbitrary tree, and the two flat forms to the conjunction or
 * disjunction {@link #condition} names. A reader that can only take a flat rule asks
 * {@link #ridesIndex}, which is false for any other shape.
 *
 * @param expr the condition over {@link #where}, by field index; never null
 */
public record WatchRule(
    String id,
    String title,
    Severity severity,
    String prefix,
    String table,
    List<Field> where,
    Condition condition,
    long minCount,
    Expr expr,
    Window window) {

  public WatchRule {
    Objects.requireNonNull(expr, "expr");
  }

  /** A rule with no window: {@link #minCount} counts within one file. */
  public WatchRule(
      String id,
      String title,
      Severity severity,
      String prefix,
      String table,
      List<Field> where,
      Condition condition,
      long minCount,
      Expr expr) {
    this(id, title, severity, prefix, table, where, condition, minCount, expr, null);
  }

  /**
   * Counting the rows a rule matches per KEY over a TIMEFRAME, rather than per file.
   *
   * <p>The distinction {@link WatchRule#minCount} does not make: {@code min_count} is a threshold
   * within one data file, an artefact of how the table was written rather than a statement about
   * time. A window's rows routinely span files, so answering one needs state the per-file scan
   * does not have; {@link io.kahshe.watch.scan.WindowScanner} holds it. A rule carrying both keys
   * is refused rather than merged.
   *
   * @param tsColumn the column carrying the event time; kahshe never guesses which one that is
   * @param timeframeMs the window's length
   * @param groupBy the columns whose values key the count; empty counts every matching row together
   * @param count how many matching rows within one window make the rule fire; at least 2, since
   *     one is what a rule without a window already means
   */
  public record Window(String tsColumn, long timeframeMs, List<String> groupBy, int count) {
    public Window {
      groupBy = List.copyOf(groupBy);
    }

    /** Every column a window needs read beyond the rule's own fields. */
    public List<String> columns() {
      List<String> columns = new ArrayList<>(groupBy.size() + 1);
      columns.add(tsColumn);
      columns.addAll(groupBy);
      return List.copyOf(columns);
    }
  }

  /** The flat forms: the condition is {@code condition} over every field. */
  public WatchRule(
      String id,
      String title,
      Severity severity,
      String prefix,
      String table,
      List<Field> where,
      Condition condition,
      long minCount) {
    this(id, title, severity, prefix, table, where, condition, minCount,
        Expr.flat(condition, where.size()));
  }

  /**
   * The condition as a tree, and only the tree: leaves index {@link #where}, a field's own values
   * stay OR'ed inside the leaf, and nothing here interprets it. Its two interpreters are
   * {@link Conditions#eval} (one row's verdict) and {@link ConfirmationSql} (the predicate an
   * operator runs to confirm the alert); {@link Conditions} documents where they must agree.
   */
  public sealed interface Expr permits Expr.FieldRef, Expr.And, Expr.Or, Expr.Not {
    record FieldRef(int index) implements Expr {}

    record And(List<Expr> terms) implements Expr {
      public And {
        terms = List.copyOf(terms);
      }
    }

    record Or(List<Expr> terms) implements Expr {
      public Or {
        terms = List.copyOf(terms);
      }
    }

    record Not(Expr term) implements Expr {}

    /**
     * {@code condition} over fields {@code 0..n-1}: what the flat forms mean. One field is the
     * field itself — {@code any-of} and {@code all-of} of one thing are the same thing, and
     * wrapping it would make a one-field rule read as non-flat and lose the index-riding path.
     */
    static Expr flat(Condition condition, int n) {
      if (n == 1) {
        return new FieldRef(0);
      }
      List<Expr> refs = new ArrayList<>();
      for (int i = 0; i < n; i++) {
        refs.add(new FieldRef(i));
      }
      return condition == Condition.ALL_OF ? new And(refs) : new Or(refs);
    }

    /** True when {@code expr} is exactly {@link #flat} of {@code condition} over {@code n} fields. */
    static boolean isFlat(Expr expr, Condition condition, int n) {
      return expr.equals(flat(condition, n));
    }

  }

  /**
   * One field predicate: a column, an operator, and the values it is tested against. Values are
   * OR'ed within a field — a Sigma selection's value list means "any of these" — and
   * {@link WatchRule#condition} joins the fields.
   */
  public record Field(String column, Op op, List<String> values) {}

  /**
   * How a field's values are tested against a value. MATCH is the only one the term index can
   * answer exactly, and CONTAINS the only one its gram set can answer at all; the other three
   * exist for the row scan, which reads the value itself and needs no index.
   */
  public enum Op {
    /** Analyzer tokens, exact: the value tokenizes to a list holding this token. */
    MATCH,
    /** Case-insensitive substring. */
    CONTAINS,
    /** The whole value: numeric on a numeric column, exact and case-sensitive on a string. */
    EQUALS,
    /**
     * The whole value, case-insensitively — {@link #EQUALS} with both sides folded. This is what
     * Sigma's plain {@code field: value} means in most implementations, and it is the operator a
     * converted Sigma rule maps to.
     *
     * <p>It compares the CANONICAL form lowercased on both sides, so {@code "07045"} in a rule
     * still meets 7045 in an int column, exactly as {@code EQUALS} does. Like the other scan-only
     * operators it never rides the index: the term index stores case-preserving canonical forms,
     * so answering this from it would need a second analyzer.
     */
    EQUALS_IGNORE_CASE,
    /** Case-insensitive prefix. */
    STARTS_WITH,
    /** Case-insensitive suffix. */
    ENDS_WITH,
    /** A regular expression (Java syntax), case-sensitive, found anywhere in the value. */
    RE,
    /** Numeric comparisons; a value that is not a number never satisfies them. */
    GT,
    GTE,
    LT,
    LTE;

    /** Whether this operator compares numbers: its rule values must parse as numbers. */
    public boolean numeric() {
      return this == GT || this == GTE || this == LT || this == LTE;
    }

    /** The spelling this operator has in the rules YAML, which is also its evidence {@code kind}. */
    public String yaml() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  public enum Severity {
    INFO,
    LOW,
    MEDIUM,
    HIGH,
    CRITICAL
  }

  public enum Condition {
    ANY_OF,
    ALL_OF
  }

  /**
   * The single-column form, as one field per value: the one place that mapping is written, so the
   * loader and every test that spells a rule out reach it the same way. {@code match: [a, b]}
   * under {@code all-of} still means both tokens, because each token is its own field.
   */
  public static WatchRule singleColumn(
      String id,
      String title,
      Severity severity,
      String prefix,
      String table,
      String column,
      List<String> match,
      List<String> contains,
      Condition condition,
      long minCount) {
    List<Field> fields = new ArrayList<>();
    for (String token : match) {
      fields.add(new Field(column, Op.MATCH, List.of(token)));
    }
    for (String literal : contains) {
      fields.add(new Field(column, Op.CONTAINS, List.of(literal)));
    }
    return new WatchRule(
        id, title, severity, prefix, table, List.copyOf(fields), condition, minCount);
  }

  /** The distinct columns this rule names, in the order the fields name them. */
  public List<String> columns() {
    return List.copyOf(new LinkedHashSet<>(where.stream().map(Field::column).toList()));
  }

  /** The one column every field names, or null when the rule spans more than one. */
  public String column() {
    List<String> columns = columns();
    return columns.size() == 1 ? columns.get(0) : null;
  }

  /**
   * Whether the index-riding evaluator can answer this rule: one column, every operator one the
   * per-file term counts or gram set can decide, and a flat condition — that evaluator combines
   * per-file evidence with {@link #condition} alone. A rule that spans columns, names an operator
   * with no index evidence, or carries a {@code not} or nested condition fires through the row
   * scan instead — never half-evaluated.
   */
  public boolean ridesIndex() {
    return whyNotRidesIndex() == null;
  }

  /**
   * Why {@link #ridesIndex} is false, or null when it is true — the one statement of the shape,
   * so a reader that must SAY why it refused a rule ({@code kahshe hunt}) reads the same clauses
   * the boolean does rather than a second copy that can drift from it. The first failing clause
   * is named; the order is the boolean's.
   */
  public String whyNotRidesIndex() {
    // A window rule never rides the index: the index answers "some file holds this token", and a
    // rate rule answered that way fires on the FIRST match -- "N within T" demoted to "ever".
    if (window != null) {
      return "a window rule: the index says a token is somewhere in a file, and 'N within T' "
          + "answered per file is 'ever'";
    }
    if (column() == null) {
      return "spans columns " + columns() + ": the index sees one column at a time";
    }
    for (Field field : where) {
      if (field.op() != Op.MATCH && field.op() != Op.CONTAINS) {
        return "operator " + field.op().yaml() + " on " + field.column()
            + ": no index tier decides it";
      }
    }
    if (!Expr.isFlat(expr, condition, where.size())) {
      return "a condition that is not a flat any-of or all-of over its fields (a not, a nesting, "
          + "or a detection expression): the index has no evidence for a field's absence in a row";
    }
    // Otherwise only a shape whose FILE-level answer is its ROW-level answer. OR commutes with
    // "some row has": a file with a somewhere or b somewhere has a row matching a or b. AND does
    // not: a row with a and a row with b is not a row with both, so all-of over two or more fields
    // is the row scan's.
    if (where.size() > 1 && condition == Condition.ALL_OF) {
      return "all-of over " + where.size() + " fields: a file holding each token somewhere is not "
          + "a row holding all of them";
    }
    return null;
  }

  /** The MATCH tokens, flattened in rule order. */
  public List<String> match() {
    return valuesOf(Op.MATCH);
  }

  /** The CONTAINS literals, flattened in rule order. */
  public List<String> contains() {
    return valuesOf(Op.CONTAINS);
  }

  private List<String> valuesOf(Op op) {
    List<String> values = new ArrayList<>();
    for (Field field : where) {
      if (field.op() == op) {
        values.addAll(field.values());
      }
    }
    return List.copyOf(values);
  }
}
