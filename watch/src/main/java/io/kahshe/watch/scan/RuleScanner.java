package io.kahshe.watch.scan;

import io.kahshe.watch.Alerts;
import io.kahshe.watch.rules.ConfirmationSql;
import io.kahshe.watch.rules.Conditions;
import io.kahshe.watch.rules.WatchRule;
import io.kahshe.watch.rules.WatchRules;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The first {@link Scanner}: watch rules, evaluated per row.
 *
 * <p>A rule is a list of field predicates over the table's columns joined by the rule's
 * condition — {@code all-of}, {@code any-of}, or the {@code detection} form's tree of
 * {@code and}/{@code or}/{@code not} over named selections — and this evaluator answers it the
 * way a detection engineer reads it: one row satisfying the whole condition, not one file
 * holding a row per conjunct. Evidence is {@code exact} because the value itself was read;
 * {@code min_count} counts matching ROWS in the file.
 *
 * <p>Every operator is answered here, including the ones no index can decide: {@code re} is a
 * compiled regular expression and {@code gt}/{@code gte}/{@code lt}/{@code lte} compare the
 * value as a number. A row whose column is null, or whose value is not a number where a numeric
 * comparison asks for one, does not satisfy that field — never an error.
 *
 * <p>Independent of the index by construction: it names the rule's columns and the pass reads
 * them, so a rule fires on a column {@code kahshe.index} never mentions. A rule the index-riding
 * {@code WatchEngine} can also answer alerts once, through the shared suppression in
 * {@link Alerts}.
 */
public final class RuleScanner implements Scanner {
  private static final Logger LOG = LoggerFactory.getLogger(RuleScanner.class);
  /** Enough to point an operator at the data; the confirmation SQL returns all of them. */
  static final int MAX_ROW_POSITIONS = 5;

  /** The name this scanner is registered and logged under. */
  public static final String NAME = "rules";

  private volatile WatchRules rules;
  private volatile Alerts alerts;

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void configure(ScanContext ctx) {
    this.rules = ctx.rules();
    this.alerts = ctx.alerts();
  }

  /**
   * Every column any loaded rule names that this table's schema actually has.
   *
   * <p>Not filtered to the rules naming this table: the seam hands a scanner the table, not the
   * catalog prefix it was loaded under, and the prefix is half of a rule's identity.
   * Over-approximating costs at most a projected column no rule on this table needs;
   * under-approximating would silently stop a rule firing. The exact rule set is selected in
   * {@link #open}, where the prefix and namespace are known.
   */
  @Override
  public Set<String> columns(TableView table) {
    WatchRules loaded = rules;
    if (loaded == null) {
      return Set.of();
    }
    Set<String> columns = new LinkedHashSet<>();
    for (WatchRule rule : loaded.current()) {
      if (rule.window() != null) {
        continue; // WindowScanner names this rule's columns, including its time and key columns
      }
      for (String column : rule.columns()) {
        if (table.has(column)) {
          columns.add(column);
        }
      }
    }
    return columns;
  }

  @Override
  public FileScan open(FileScanContext ctx) {
    WatchRules loaded = rules;
    Alerts sink = alerts;
    if (loaded == null || sink == null) {
      return null;
    }
    String qualified = ctx.namespace() + "." + ctx.tableName();
    List<PreparedRule> prepared = new ArrayList<>();
    for (WatchRule rule : loaded.current()) {
      if (!rule.prefix().equals(ctx.prefix()) || !rule.table().equals(qualified)) {
        continue;
      }
      // A window rule belongs to WindowScanner alone. Answered here as well it would fire on the
      // FIRST matching row — a rate rule silently demoted to a match rule.
      if (rule.window() != null) {
        continue;
      }
      // A column the projection lacks means the schema lacks it: discovery counts and logs that
      // rule as uncovered, and half-evaluating it here would be worse than not firing.
      if (!rule.columns().stream().allMatch(c -> ctx.kinds().containsKey(c))) {
        continue;
      }
      try {
        prepared.add(PreparedRule.of(rule, ctx.kinds(), ctx.repeated()));
      } catch (PreparedRule.UnsupportedRule e) {
        // Skipped, not fatal: one unanswerable rule must not stop the others on this table.
        LOG.warn("watch rule {} skipped on {}: {}", rule.id(), ctx.tableName(), e.getMessage());
      }
    }
    return prepared.isEmpty() ? null : new Scan(prepared, ctx, sink);
  }

  /** One file's tally for every rule on this table, and the alerts they raise at the end. */
  private static final class Scan implements FileScan {
    private final List<PreparedRule> rules;
    private final List<State> states = new ArrayList<>();
    private final FileScanContext ctx;
    private final Alerts alerts;
    private int rowCount;

    Scan(List<PreparedRule> rules, FileScanContext ctx, Alerts alerts) {
      this.rules = rules;
      this.ctx = ctx;
      this.alerts = alerts;
      for (PreparedRule rule : rules) {
        states.add(new State(rule));
      }
    }

    @Override
    public void row(int rowPosition, Row row) {
      rowCount = rowPosition + 1;
      for (State state : states) {
        try {
          state.evaluate(row, rowPosition);
        } catch (RuntimeException e) {
          // a poisoned rule must not take down the others (or the file)
          LOG.warn("watch rule {} failed on row {} of {}", state.rule.rule().id(), rowPosition,
              ctx.path(), e);
        }
      }
    }

    @Override
    public List<Map<String, Object>> finish() {
      List<Map<String, Object>> raised = new ArrayList<>();
      for (State state : states) {
        if (state.matchedRows >= state.rule.rule().minCount()) {
          raised.add(payload(state));
        }
      }
      return raised;
    }

    private Map<String, Object> payload(State state) {
      WatchRule rule = state.rule.rule();
      List<Map<String, Object>> evidence = new ArrayList<>();
      for (int i = 0; i < state.rule.fields().size(); i++) {
        PreparedRule.Field field = state.rule.fields().get(i);
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("kind", field.field().op().yaml());
        entry.put("column", field.field().column());
        entry.put("values", field.field().values());
        entry.put("count", state.fieldRows[i]);
        entry.put("row_positions", List.copyOf(state.fieldPositions.get(i)));
        // The row is the verdict here: this evaluator read the value, not a summary of it.
        entry.put("confidence", ctx.confidence());
        evidence.add(entry);
      }
      String sql = ConfirmationSql.build(alerts.sqlCatalog(), ctx.namespace(), ctx.tableName(),
          ctx.snapshotId(), ctx.path(), rule.where(), state.rule.numericColumns(), rule.expr());
      Map<String, Object> extra = new LinkedHashMap<>();
      extra.put("matched_rows", state.matchedRows);
      extra.put("matched_row_positions", List.copyOf(state.positions));
      LOG.warn("WATCH ALERT rule={} severity={} prefix={} table={}.{} columns={} file={} "
              + "snapshot={} build_kind=scan matched_rows={}",
          rule.id(), rule.severity().name().toLowerCase(Locale.ROOT), ctx.prefix(),
          ctx.namespace(), ctx.tableName(), rule.columns(), ctx.path(), ctx.snapshotId(),
          state.matchedRows);
      return alerts.payload(rule, ctx.prefix(), ctx.namespace(), ctx.tableName(), ctx.snapshotId(),
          ctx.path(), rowCount, evidence, extra, "scan", false, sql);
    }
  }

  /** One rule's tally over one file: matching rows, and where the first few of them are. */
  private static final class State {
    private final PreparedRule rule;
    private final long[] fieldRows;
    /** Reused across rows: the row path allocates nothing. */
    private final boolean[] hits;
    private final List<List<Integer>> fieldPositions = new ArrayList<>();
    private final List<Integer> positions = new ArrayList<>();
    private long matchedRows;

    State(PreparedRule rule) {
      this.rule = rule;
      this.fieldRows = new long[rule.fields().size()];
      this.hits = new boolean[rule.fields().size()];
      for (int i = 0; i < rule.fields().size(); i++) {
        fieldPositions.add(new ArrayList<>());
      }
    }

    void evaluate(Row row, int position) {
      rule.hits(row, hits);
      for (int i = 0; i < hits.length; i++) {
        if (hits[i]) {
          fieldRows[i]++;
          if (fieldPositions.get(i).size() < MAX_ROW_POSITIONS) {
            fieldPositions.get(i).add(position);
          }
        }
      }
      // The tree, not the flat condition: a detection rule's condition is an expression over the
      // selections, and the flat forms parse to the tree that means what they meant.
      if (!Conditions.eval(rule.rule().expr(), hits)) {
        return;
      }
      matchedRows++;
      if (positions.size() < MAX_ROW_POSITIONS) {
        positions.add(position);
      }
    }
  }
}
