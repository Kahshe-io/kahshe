package io.kahshe.watch.rules;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.kahshe.common.Metrics;
import io.kahshe.analysis.analyzer.Analyzer;
import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads watch rules from the YAML file at KAHSHE_WATCH_RULES. Validation is per-rule and
 * fail-open: an invalid rule is skipped with a loud WARN and counted, the rest of the file
 * loads. Hot reload: the file's mtime is re-checked on every {@link #current()} call (each build
 * start and each discovery poll); a file-level parse error keeps the previous rule set.
 *
 * <p>Three rule forms load here. The single-column form ({@code column} with {@code match} and
 * {@code contains}) parses to one {@link WatchRule.Field} per value; the multi-field form
 * ({@code where}: a list of {@code column} + operator entries) spans columns; the
 * {@code detection} form is Sigma's shape — named selections, each a list of the same entries,
 * and a {@code condition} expression over their names ({@link ConditionParser}). They are
 * exclusive — a rule carrying two of them is refused, not merged.
 */
public final class WatchRules {
  private static final Logger LOG = LoggerFactory.getLogger(WatchRules.class);
  private static final Set<String> FIELDS =
      Set.of("id", "title", "severity", "prefix", "table", "column", "match", "contains",
          "where", "condition", "min_count", "detection", "window");
  /** A {@code window} block: the time column, the length, the key, and the threshold. */
  private static final Set<String> WINDOW_FIELDS =
      Set.of("ts_column", "timeframe", "group_by", "count");

  /** A {@code where} entry names one column and one or more operators over it. */
  private static final Set<String> WHERE_FIELDS =
      Set.of("column", "match", "contains", "equals", "equals_ignore_case", "starts_with",
          "ends_with", "re", "gt",
          "gte", "lt", "lte");
  /** In the order an entry's operators become fields, so a rule's evidence reads as written. */
  private static final List<WatchRule.Op> OPS =
      List.of(WatchRule.Op.MATCH, WatchRule.Op.CONTAINS, WatchRule.Op.EQUALS,
          WatchRule.Op.EQUALS_IGNORE_CASE, WatchRule.Op.STARTS_WITH, WatchRule.Op.ENDS_WITH,
          WatchRule.Op.RE, WatchRule.Op.GT, WatchRule.Op.GTE, WatchRule.Op.LT, WatchRule.Op.LTE);

  private final File file;
  private final Metrics metrics;
  private volatile List<WatchRule> rules = List.of();
  private volatile long loadedMtime = -1;

  public WatchRules(String path, Metrics metrics) {
    this.file = path == null || path.isBlank() ? null : new File(path);
    this.metrics = metrics;
    metrics.watchRulesLoaded = () -> rules.size();
    reloadIfChanged();
  }

  /** Test seam: a fixed rule set, no file. */
  static WatchRules fixed(List<WatchRule> fixedRules, Metrics metrics) {
    WatchRules result = new WatchRules("", metrics);
    result.rules = List.copyOf(fixedRules);
    return result;
  }

  public List<WatchRule> current() {
    reloadIfChanged();
    return rules;
  }

  /**
   * Rules grouped by the table they name, keyed {@code prefix|table}, in the order given. Static
   * so a test that mocks this class and stubs {@link #current} still groups.
   */
  public static Map<String, List<WatchRule>> byTable(List<WatchRule> rules) {
    Map<String, List<WatchRule>> byTable = new LinkedHashMap<>();
    for (WatchRule rule : rules) {
      byTable.computeIfAbsent(rule.prefix() + "|" + rule.table(), k -> new ArrayList<>()).add(rule);
    }
    return byTable;
  }

  private synchronized void reloadIfChanged() {
    if (file == null) {
      return;
    }
    long mtime = file.lastModified();
    if (mtime == loadedMtime) {
      return;
    }
    // claim the mtime either way: a broken file is retried on its next change, not every call
    loadedMtime = mtime;
    try {
      List<WatchRule> parsed = parse(new YAMLMapper().readTree(file));
      rules = parsed;
      LOG.info("loaded {} watch rules from {}", parsed.size(), file);
    } catch (Exception e) {
      LOG.warn("watch rules file {} unreadable; keeping previous {} rules", file, rules.size(), e);
    }
  }

  private List<WatchRule> parse(JsonNode root) {
    JsonNode list = root.has("rules") ? root.path("rules") : root;
    if (!list.isArray()) {
      throw new IllegalArgumentException("expected a top-level 'rules' list");
    }
    List<WatchRule> result = new ArrayList<>();
    java.util.Set<String> seen = new java.util.HashSet<>();
    int i = 0;
    for (JsonNode node : list) {
      WatchRule rule = parseRule(node, i++);
      if (rule == null) {
        continue;
      }
      // rule ids key alert dedup; a colliding second rule would silently share the first's state
      if (!seen.add(rule.id())) {
        skip(rule.id(), "duplicate id (first occurrence wins)");
        continue;
      }
      result.add(rule);
    }
    return List.copyOf(result);
  }

  private WatchRule parseRule(JsonNode node, int index) {
    String id = node.path("id").asText("");
    String label = id.isBlank() ? "#" + index : id;
    if (!node.isObject()) {
      return skip(label, "rule is not a mapping");
    }
    for (Iterator<String> it = node.fieldNames(); it.hasNext(); ) {
      String field = it.next();
      if (!FIELDS.contains(field)) {
        return skip(label, "unknown field '" + field + "'");
      }
    }
    if (id.isBlank()) {
      return skip(label, "missing id");
    }
    WatchRule.Severity severity;
    try {
      severity =
          WatchRule.Severity.valueOf(
              node.path("severity").asText("").toUpperCase(Locale.ROOT).replace('-', '_'));
    } catch (IllegalArgumentException e) {
      return skip(label, "severity must be one of info|low|medium|high|critical");
    }
    String prefix = node.path("prefix").asText("");
    if (prefix.isBlank()) {
      return skip(label, "missing prefix");
    }
    String table = node.path("table").asText("");
    if (!table.contains(".") || table.contains("*") || table.contains("%")) {
      return skip(label, "table must be 'ns.table' with no wildcards");
    }
    long minCount = node.path("min_count").asLong(1);
    if (minCount < 1) {
      return skip(label, "min_count must be >= 1");
    }
    String title = node.path("title").asText(id);
    WatchRule.Window window = null;
    if (node.has("window")) {
      // Refused rather than merged: min_count counts rows in one FILE and a window counts them
      // over TIME. A rule carrying both would have two thresholds and no way to say which won.
      if (node.has("min_count")) {
        return skip(label, "a rule declares 'min_count' (a count within one data file) or "
            + "'window' (a count over a timeframe), never both");
      }
      window = parseWindow(node.path("window"), label);
      if (window == null) {
        return null;
      }
    }
    if (node.has("detection")) {
      // Exclusive with the other two forms AND with the flat condition: the detection form's
      // condition is an expression inside it, and a top-level any-of/all-of beside it would be
      // two answers to one question.
      for (String other : List.of("column", "where", "match", "contains", "condition")) {
        if (node.has(other)) {
          return skip(label, "a rule with 'detection' carries its columns and condition inside it; "
              + "'" + other + "' does not belong beside it");
        }
      }
      return parseDetection(node.path("detection"), id, title, severity, prefix, table, minCount,
          window, label);
    }
    String conditionText = node.path("condition").asText("any-of");
    WatchRule.Condition condition;
    switch (conditionText) {
      case "any-of" -> condition = WatchRule.Condition.ANY_OF;
      case "all-of" -> condition = WatchRule.Condition.ALL_OF;
      default -> {
        return skip(label, "condition must be any-of or all-of");
      }
    }
    boolean multiField = node.has("where");
    // The two forms are exclusive rather than merged: a rule carrying both would have two answers
    // to "which columns does this name", and the quiet one would win.
    if (multiField && node.has("column")) {
      return skip(label, "a rule declares 'column' (one column) or 'where' (fields across "
          + "columns), never both");
    }
    if (multiField && (node.has("match") || node.has("contains"))) {
      return skip(label, "with 'where', match and contains belong inside its entries");
    }
    if (multiField) {
      List<WatchRule.Field> where = parseWhere(node.path("where"), label);
      if (where == null) {
        return null;
      }
      return new WatchRule(id, title, severity, prefix, table, where, condition, minCount,
          WatchRule.Expr.flat(condition, where.size()), window);
    }
    String column = node.path("column").asText("");
    if (column.isBlank()) {
      return skip(label, "missing column");
    }
    List<String> match = new ArrayList<>();
    for (JsonNode entry : node.path("match")) {
      String token = matchToken(entry.asText(), label);
      if (token == null) {
        return null;
      }
      match.add(token);
    }
    List<String> contains = new ArrayList<>();
    for (JsonNode entry : node.path("contains")) {
      String raw = entry.asText();
      if (raw.length() < 3) {
        return skip(label, "contains literal '" + raw + "' shorter than 3 chars");
      }
      contains.add(raw);
    }
    if (match.isEmpty() && contains.isEmpty()) {
      return skip(label, "rule needs at least one match token or contains literal");
    }
    // Only in the single-column form, where min_count counts token occurrences on the
    // index-riding path and there are none to count without match tokens. Under 'where' it counts
    // matching rows, which every operator produces.
    if (minCount > 1 && match.isEmpty()) {
      return skip(label, "min_count applies to match tokens only");
    }
    WatchRule flat =
        WatchRule.singleColumn(
            id, title, severity, prefix, table, column, match, contains, condition, minCount);
    return window == null
        ? flat
        : new WatchRule(id, title, severity, prefix, table, flat.where(), condition, minCount,
            flat.expr(), window);
  }

  /**
   * The {@code detection} mapping: every key but {@code condition} names a selection, a list of
   * {@code where} entries AND'ed together, and {@code condition} is the expression over those
   * names. The rule's field list is the selections' fields in order, so evidence reads as written
   * and the tree indexes into it. {@link WatchRule#condition} is only a summary — accurate when
   * the tree is flat, nominal otherwise; {@link WatchRule#ridesIndex} tells the two apart.
   */
  private WatchRule parseDetection(JsonNode detection, String id, String title,
      WatchRule.Severity severity, String prefix, String table, long minCount,
      WatchRule.Window window, String label) {
    if (!detection.isObject()) {
      return skip(label, "'detection' must be a mapping of selections plus a condition");
    }
    String conditionText = detection.path("condition").asText("");
    if (conditionText.isBlank()) {
      return skip(label, "'detection' needs a condition");
    }
    List<WatchRule.Field> fields = new ArrayList<>();
    Map<String, WatchRule.Expr> selections = new LinkedHashMap<>();
    for (Iterator<String> it = detection.fieldNames(); it.hasNext(); ) {
      String name = it.next();
      if (name.equals("condition")) {
        continue;
      }
      if (!name.matches("[A-Za-z_][A-Za-z0-9_]*")) {
        return skip(label, "selection name '" + name + "' must be a word (letters, digits, _)");
      }
      List<WatchRule.Field> selectionFields = parseWhere(detection.path(name), label);
      if (selectionFields == null) {
        return null;
      }
      List<WatchRule.Expr> refs = new ArrayList<>();
      for (int i = 0; i < selectionFields.size(); i++) {
        refs.add(new WatchRule.Expr.FieldRef(fields.size() + i));
      }
      fields.addAll(selectionFields);
      selections.put(name, refs.size() == 1 ? refs.get(0) : new WatchRule.Expr.And(refs));
    }
    if (selections.isEmpty()) {
      return skip(label, "'detection' names no selection");
    }
    WatchRule.Expr expr;
    try {
      expr = ConditionParser.parse(conditionText, selections);
    } catch (IllegalArgumentException e) {
      return skip(label, e.getMessage());
    }
    WatchRule.Condition summary =
        WatchRule.Expr.isFlat(expr, WatchRule.Condition.ANY_OF, fields.size())
            ? WatchRule.Condition.ANY_OF
            : WatchRule.Condition.ALL_OF;
    return new WatchRule(id, title, severity, prefix, table, List.copyOf(fields), summary,
        minCount, expr, window);
  }

  /**
   * The {@code window} block. Every field is required but {@code group_by}: kahshe does not guess
   * which column carries event time, and a threshold of one is what a rule without a window
   * already means.
   */
  private WatchRule.Window parseWindow(JsonNode node, String label) {
    if (!node.isObject()) {
      skip(label, "'window' must be a mapping (ts_column, timeframe, count, optional group_by)");
      return null;
    }
    for (Iterator<String> it = node.fieldNames(); it.hasNext(); ) {
      String field = it.next();
      if (!WINDOW_FIELDS.contains(field)) {
        skip(label, "unknown 'window' field '" + field + "'");
        return null;
      }
    }
    String tsColumn = node.path("ts_column").asText("");
    if (tsColumn.isBlank()) {
      skip(label, "'window' needs ts_column: which column carries event time is not guessable");
      return null;
    }
    long timeframeMs = durationMs(node.path("timeframe").asText(""));
    if (timeframeMs <= 0) {
      skip(label, "'window' needs a positive timeframe like 30s, 5m, 2h or 1d");
      return null;
    }
    int count = node.path("count").asInt(0);
    if (count < 2) {
      skip(label, "'window' count must be at least 2; a threshold of one is a rule without a window");
      return null;
    }
    List<String> groupBy = new ArrayList<>();
    for (JsonNode entry : node.path("group_by")) {
      String column = entry.asText("");
      if (column.isBlank()) {
        skip(label, "'window' group_by has an empty column");
        return null;
      }
      groupBy.add(column);
    }
    return new WatchRule.Window(tsColumn, timeframeMs, groupBy, count);
  }

  /** {@code 500ms}, {@code 30s}, {@code 5m}, {@code 2h}, {@code 1d}; 0 when unparseable. */
  static long durationMs(String text) {
    java.util.regex.Matcher m =
        java.util.regex.Pattern.compile("^(\\d+)(ms|s|m|h|d)$").matcher(text.trim());
    if (!m.matches()) {
      return 0;
    }
    long n = Long.parseLong(m.group(1));
    return switch (m.group(2)) {
      case "ms" -> n;
      case "s" -> n * 1000;
      case "m" -> n * 60_000;
      case "h" -> n * 3_600_000;
      default -> n * 86_400_000;
    };
  }

  /** The {@code where} list: one entry per column, one field per operator named in it. */
  private List<WatchRule.Field> parseWhere(JsonNode list, String label) {
    if (!list.isArray() || list.isEmpty()) {
      skip(label, "'where' must be a non-empty list of field entries");
      return null;
    }
    List<WatchRule.Field> fields = new ArrayList<>();
    for (JsonNode entry : list) {
      if (!entry.isObject()) {
        skip(label, "'where' entry is not a mapping");
        return null;
      }
      for (Iterator<String> it = entry.fieldNames(); it.hasNext(); ) {
        String field = it.next();
        if (!WHERE_FIELDS.contains(field)) {
          skip(label, "unknown 'where' field '" + field + "'");
          return null;
        }
      }
      String column = entry.path("column").asText("");
      if (column.isBlank()) {
        skip(label, "'where' entry is missing column");
        return null;
      }
      int before = fields.size();
      for (WatchRule.Op op : OPS) {
        JsonNode values = entry.path(op.yaml());
        if (values.isMissingNode() || values.isNull()) {
          continue;
        }
        List<String> parsed = parseValues(values, op, column, label);
        if (parsed == null) {
          return null;
        }
        fields.add(new WatchRule.Field(column, op, parsed));
      }
      if (fields.size() == before) {
        skip(label, "'where' entry on column " + column + " names no operator "
            + "(match|contains|equals|equals_ignore_case|starts_with|ends_with|re|gt|gte|lt|lte)");
        return null;
      }
    }
    return List.copyOf(fields);
  }

  /**
   * One operator's values, OR'ed within the field. A scalar is accepted as a one-element list
   * because a Sigma field with a single value is written that way far more often than not, and a
   * number stays its text: the scan canonicalises it against the column's type, so {@code 7045}
   * and {@code "7045"} are the same literal on an int column.
   */
  private List<String> parseValues(JsonNode node, WatchRule.Op op, String column, String label) {
    List<JsonNode> raw = new ArrayList<>();
    if (node.isArray()) {
      node.forEach(raw::add);
    } else {
      raw.add(node);
    }
    if (raw.isEmpty()) {
      skip(label, op.yaml() + " on column " + column + " has no values");
      return null;
    }
    List<String> values = new ArrayList<>();
    for (JsonNode value : raw) {
      // `no`, `off`, `yes` and `true` are BOOLEANS in YAML, so a rule matching the literal string
      // "no" would match "false" instead: it loads without complaint, reviews as correct, and
      // never fires. Refused rather than coerced, with the fix in the message. (`0x1f` -> 31 has
      // the same shape but is indistinguishable from an ordinary integer once parsed, so quote
      // identifiers. `1.10` -> 1.1 is benign: the decimal canonical form strips trailing zeros.)
      if (value.isBoolean()) {
        skip(label, op.yaml() + " on column " + column + " has the value " + value.asText()
            + ", which YAML read as a boolean from something like no/off/yes/true. Quote it "
            + "(\"" + value.asText() + "\") if you meant the text");
        return null;
      }
      String text = value.asText();
      if (text.isEmpty()) {
        skip(label, op.yaml() + " on column " + column + " has an empty value");
        return null;
      }
      if (op == WatchRule.Op.MATCH) {
        String token = matchToken(text, label);
        if (token == null) {
          return null;
        }
        values.add(token);
        continue;
      }
      if (op == WatchRule.Op.CONTAINS && text.length() < 3) {
        skip(label, "contains literal '" + text + "' shorter than 3 chars");
        return null;
      }
      if (op == WatchRule.Op.RE) {
        try {
          java.util.regex.Pattern.compile(text);
        } catch (java.util.regex.PatternSyntaxException e) {
          skip(label, "re on column " + column + " is not a valid regular expression: "
              + e.getDescription());
          return null;
        }
      }
      if (op.numeric()) {
        try {
          new java.math.BigDecimal(text);
        } catch (NumberFormatException e) {
          skip(label, op.yaml() + " on column " + column + " needs a number, not '" + text + "'");
          return null;
        }
      }
      values.add(text);
    }
    return List.copyOf(values);
  }

  /**
   * A match entry normalized to the one token it must be, or null — already counted and logged.
   *
   * <p>Validated against the current tokens contract at the default cap: the file is loaded with
   * no index in hand to ask, so a rule naming a token some column's index would admit but this
   * default would not is refused here. The contract decides indexability, not a plain alphabet
   * check: under v3 a compound identifier (an IP, a UUID, a dashed id) is one token.
   */
  private String matchToken(String raw, String label) {
    Analyzer.Contract contract =
        Analyzer.contract(Analyzer.Kind.TOKENS, Analyzer.DEFAULT_MAX_TOKEN_LEN);
    List<String> tokens = contract.queryTerms(raw);
    if (tokens.size() != 1 || !contract.isIndexable(tokens.get(0))) {
      skip(label, "match entry '" + raw + "' is not a single indexable analyzer token");
      return null;
    }
    return tokens.get(0);
  }

  private WatchRule skip(String label, String reason) {
    metrics.watchRulesSkipped.increment();
    LOG.warn("watch rule {} SKIPPED: {}", label, reason);
    return null;
  }
}
