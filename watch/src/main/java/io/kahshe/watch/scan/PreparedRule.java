package io.kahshe.watch.scan;

import io.kahshe.analysis.Canonical;
import io.kahshe.analysis.ValueKind;
import java.util.Map;
import io.kahshe.watch.rules.Conditions;
import io.kahshe.watch.rules.WatchRule;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * One rule resolved against a table's schema, and the per-row test that follows from it.
 *
 * <p>Prepared ONCE per file rather than per row: literals canonicalised against the column type,
 * regular expressions compiled, comparison values parsed. The row path then does no allocation
 * and no parsing — it reads the row's memoized forms and compares.
 *
 * <p>Shared by the two scanners that need a row verdict: {@link RuleScanner}, which alerts on the
 * rows themselves, and {@link WindowScanner}, which counts them per key over a timeframe. They
 * must agree on what "this row matches" means, which is why there is one of these rather than two
 * copies.
 */
public final class PreparedRule {

  /** One field with its values already resolved, plus whatever that operator needs compiled. */
  record Field(
      WatchRule.Field field, List<String> values, List<Pattern> patterns, List<BigDecimal> numbers) {}

  private final WatchRule rule;
  private final List<Field> fields;
  private final Set<String> numericColumns;

  private PreparedRule(WatchRule rule, List<Field> fields, Set<String> numericColumns) {
    this.rule = rule;
    this.fields = fields;
    this.numericColumns = numericColumns;
  }

  public WatchRule rule() {
    return rule;
  }

  List<Field> fields() {
    return fields;
  }

  /** The columns whose literals are compared as numbers, for the confirmation SQL. */
  public Set<String> numericColumns() {
    return numericColumns;
  }

  /** How many fields a {@link #hits} array must hold. */
  public int fieldCount() {
    return fields.size();
  }

  /**
   * Resolves a rule against what the pass will hand it: each named column's {@link ValueKind}.
   *
   * <p>Kinds rather than a table format's types, so this class — the row test both scanners share
   * — binds to no table format at all. A column the reader does not know is
   * {@link ValueKind#OTHER}, which compares as text and never as a number.
   */
  public static PreparedRule of(WatchRule rule, Map<String, ValueKind> kinds) {
    return of(rule, kinds, Set.of());
  }

  /**
   * As above, told which columns hold many values per row.
   *
   * @throws UnsupportedRule when the rule would need a quantifier this evaluator does not have
   */
  public static PreparedRule of(
      WatchRule rule, Map<String, ValueKind> kinds, Set<String> repeated) {
    refuseCrossMemberConjunction(rule, repeated);
    List<Field> prepared = new ArrayList<>();
    Set<String> numeric = new LinkedHashSet<>();
    Set<String> textual = new LinkedHashSet<>();
    for (WatchRule.Field field : rule.where()) {
      ValueKind kind = kinds.getOrDefault(field.column(), ValueKind.OTHER);
      List<String> values = new ArrayList<>();
      for (String value : field.values()) {
        values.add(switch (field.op()) {
          // Through the same canonical form the value goes through, so 7045 on an int column and
          // "07045" in the rule are one literal.
          case EQUALS -> {
            String canonical = Canonical.form(kind, value);
            yield canonical == null ? value : canonical;
          }
          // The same canonical form, then folded — so "07045" meets 7045 case-insensitively too,
          // rather than the fold quietly costing the numeric equivalence EQUALS has.
          case EQUALS_IGNORE_CASE -> {
            String canonical = Canonical.form(kind, value);
            yield (canonical == null ? value : canonical).toLowerCase(Locale.ROOT);
          }
          // A regex is case-sensitive and a number is neither cased nor lowercased; only the
          // three case-insensitive text operators are folded here.
          case MATCH, RE, GT, GTE, LT, LTE -> value;
          default -> value.toLowerCase(Locale.ROOT);
        });
      }
      if (field.op() == WatchRule.Op.EQUALS
          || field.op() == WatchRule.Op.EQUALS_IGNORE_CASE) {
        boolean asNumber = kind.numeric() && values.stream().allMatch(PreparedRule::isNumber);
        (asNumber ? numeric : textual).add(field.column());
      } else if (field.op().numeric() && kind.numeric()) {
        // A numeric comparison on a numeric column needs no cast in the confirmation SQL.
        numeric.add(field.column());
      }
      List<Pattern> patterns = null;
      List<BigDecimal> numbers = null;
      if (field.op() == WatchRule.Op.RE) {
        patterns = new ArrayList<>();
        for (String value : values) {
          patterns.add(Pattern.compile(value)); // the loader compiled it once already
        }
      } else if (field.op().numeric()) {
        numbers = new ArrayList<>();
        for (String value : values) {
          numbers.add(new BigDecimal(value)); // likewise validated at load
        }
      }
      prepared.add(new Field(field, List.copyOf(values), patterns, numbers));
    }
    // A column carrying one numeric equals and one non-numeric one is quoted: an unquoted literal
    // the engine cannot parse would make the whole confirmation SQL unrunnable.
    numeric.removeAll(textual);
    return new PreparedRule(rule, List.copyOf(prepared), Set.copyOf(numeric));
  }

  /** A rule this evaluator will not answer, rather than answer differently than it reads. */
  public static final class UnsupportedRule extends RuntimeException {
    UnsupportedRule(String message) {
      super(message);
    }
  }

  /**
   * Refuses a rule whose truth would depend on a quantifier this evaluator does not have.
   *
   * <p>An operator on a container column is tested per member and satisfied by ANY member, so two
   * fields on the SAME container column ask {@code ∃m:p ∧ ∃m:q}. An engine's {@code any_match}
   * asks {@code ∃m:(p ∧ q)} — one member satisfying both — and the two disagree exactly when the
   * literals match different members. {@code cmd_argv|contains|all: ['powershell','-enc']} fires
   * on a row whose members are {@code ["/bin/sh -c echo powershell", "notepad.exe -enc AAA"]},
   * where neither member is the command the rule describes. The confirmation SQL is generated the
   * same per-field way, so it CONFIRMS the wrong alert rather than exposing it.
   *
   * <p>Refused conservatively: two fields on one container column, whatever the condition joins
   * them with. A disjunction of them is sound, but it is also spelled as one field with two values
   * — which is what the message says — so the cost of over-refusing is a rewrite rather than a
   * capability, and the alternative is reading the condition tree to decide which pairs are
   * actually conjoined.
   *
   * <p>Loud rather than quiet, because the failure it prevents is an alert that looks confirmed.
   */
  private static void refuseCrossMemberConjunction(WatchRule rule, Set<String> repeated) {
    if (repeated.isEmpty()) {
      return;
    }
    Set<String> seen = new LinkedHashSet<>();
    for (WatchRule.Field field : rule.where()) {
      if (repeated.contains(field.column()) && !seen.add(field.column())) {
        throw new UnsupportedRule(
            "rule " + rule.id() + " names the repeated column '" + field.column() + "' in more "
                + "than one condition. Each operator on a list or map is satisfied by ANY member, "
                + "so several of them together do not mean 'one member satisfies all' — which is "
                + "what the rule reads as and what its confirmation SQL would claim. Put the "
                + "values in ONE condition on that column if any of them may match, or name a "
                + "scalar column for the part that must hold of the same value.");
      }
    }
  }

  /**
   * Fills {@code hits} with which fields this row satisfied.
   *
   * @param row the row to test
   * @param hits an array of {@link #fieldCount} booleans, overwritten
   */
  public void hits(Row row, boolean[] hits) {
    for (int i = 0; i < fields.size(); i++) {
      hits[i] = matches(fields.get(i), row);
    }
  }

  /** Whether this row satisfies the whole rule; {@code hits} is scratch the caller owns. */
  public boolean matches(Row row, boolean[] hits) {
    hits(row, hits);
    return Conditions.eval(rule.expr(), hits);
  }

  /**
   * Whether any of this column's values satisfies the field.
   *
   * <p>A scalar has one value and this is the test it always was. A list or a map has one per
   * member, and the field is satisfied if ANY member satisfies it — the same existential the index
   * answers (FORMAT.md §6.7), so a rule means the same thing whether it rode a build or a scan.
   * Members are never joined into one string: the operator sees each value as it is.
   */
  private static boolean matches(Field field, Row row) {
    String column = field.field().column();
    int arity = row.arity(column);
    for (int member = 0; member < arity; member++) {
      if (matchesMember(field, row, column, member)) {
        return true;
      }
    }
    return false;
  }

  private static boolean matchesMember(Field field, Row row, String column, int member) {
    String canonical = row.canonical(column, member);
    if (canonical == null) {
      return false;
    }
    BigDecimal number = field.field().op().numeric() ? number(canonical) : null;
    for (int i = 0; i < field.values().size(); i++) {
      String value = field.values().get(i);
      boolean hit = switch (field.field().op()) {
        case MATCH -> row.tokens(column, member).contains(value);
        case CONTAINS -> row.lowered(column, member).contains(value);
        case EQUALS -> canonical.equals(value);
        // Both sides are the canonical form folded: the value was canonicalised and
        // lowered at load, and row.lowered is exactly canonical().toLowerCase().
        case EQUALS_IGNORE_CASE -> row.lowered(column, member).equals(value);
        case STARTS_WITH -> row.lowered(column, member).startsWith(value);
        case ENDS_WITH -> row.lowered(column, member).endsWith(value);
        case RE -> field.patterns().get(i).matcher(canonical).find();
        // A value that is not a number satisfies no comparison, rather than failing the row.
        case GT -> number != null && number.compareTo(field.numbers().get(i)) > 0;
        case GTE -> number != null && number.compareTo(field.numbers().get(i)) >= 0;
        case LT -> number != null && number.compareTo(field.numbers().get(i)) < 0;
        case LTE -> number != null && number.compareTo(field.numbers().get(i)) <= 0;
      };
      if (hit) {
        return true;
      }
    }
    return false;
  }

  private static BigDecimal number(String canonical) {
    try {
      return new BigDecimal(canonical);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static boolean isNumber(String value) {
    try {
      new BigDecimal(value);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }
}
