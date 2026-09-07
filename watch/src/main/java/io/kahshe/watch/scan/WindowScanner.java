package io.kahshe.watch.scan;

import io.kahshe.common.Metrics;
import io.kahshe.watch.Alerts;
import io.kahshe.watch.rules.ConfirmationSql;
import io.kahshe.watch.rules.WatchRule;
import io.kahshe.watch.rules.WatchRules;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import io.kahshe.analysis.ValueKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rate detection: {@code N matching rows for one key within T}, counted ACROSS files.
 *
 * <p>A scanner of its own rather than a flag on the rule one, because a window cannot be answered
 * per file: five failed logons in five minutes routinely arrive in three different files, so a
 * per-file count undercounts the window and the rule silently does not fire. The state that
 * answers it lives here, and so do the costs it brings.
 *
 * <p>Memory is {@code N} longs per live key per rule, capped by
 * {@code KAHSHE_WATCH_WINDOW_MAX_KEYS}. An eviction and an out-of-order drop each lose a partial
 * window — a MISS rather than a delay — and both are counted. State is in memory only, so a
 * restart loses every partial window; that is said plainly in the startup log, and what rebuilds
 * it is {@link Scanner#replayMs}.
 */
public final class WindowScanner implements Scanner {
  private static final Logger LOG = LoggerFactory.getLogger(WindowScanner.class);

  /** The name this scanner is registered and logged under. */
  public static final String NAME = "window";

  /** The key of a rule that groups by nothing: every matching row counts together. */
  private static final String UNGROUPED = "*";

  private volatile WatchRules rules;
  private volatile Alerts alerts;
  private volatile Metrics metrics = new Metrics();
  private volatile int maxKeys = 200_000;
  /** Per rule id, the live windows. */
  private final Map<String, WindowCounters> byRule = new ConcurrentHashMap<>();

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void configure(ScanContext ctx) {
    this.rules = ctx.rules();
    this.alerts = ctx.alerts();
    this.metrics = ctx.metrics();
    this.maxKeys = ctx.windowMaxKeys();
    ctx.metrics().watchWindowKeys = this::liveKeys;
    LOG.info("window rules: up to {} live keys per rule, in memory only — a restart loses every "
        + "partial window, which is a missed detection rather than a delayed one", maxKeys);
  }

  private long liveKeys() {
    long total = 0;
    for (WindowCounters counters : byRule.values()) {
      total += counters.keys();
    }
    return total;
  }

  @Override
  public Set<String> columns(TableView table) {
    WatchRules loaded = rules;
    if (loaded == null) {
      return Set.of();
    }
    Set<String> columns = new LinkedHashSet<>();
    for (WatchRule rule : loaded.current()) {
      if (rule.window() == null) {
        continue;
      }
      List<String> needed = new ArrayList<>(rule.columns());
      needed.addAll(rule.window().columns());
      // All or nothing per rule: a window rule missing its ts column cannot be half-answered.
      if (needed.stream().allMatch(table::has)) {
        columns.addAll(needed);
      }
    }
    return columns;
  }

  /**
   * The time column this table's window rules count in. Rules naming DIFFERENT time columns on
   * one table cannot both be ordered, so the first is chosen and the rest are named in a warning:
   * ordering by one of them is strictly better than ordering by none, and the counter still shows
   * what the others lose.
   */
  @Override
  public String orderRowsBy(TableView table) {
    WatchRules loaded = rules;
    if (loaded == null) {
      return null;
    }
    List<String> named = new ArrayList<>();
    for (WatchRule rule : loaded.current()) {
      if (rule.window() == null) {
        continue;
      }
      List<String> needed = new ArrayList<>(rule.columns());
      needed.addAll(rule.window().columns());
      if (needed.stream().allMatch(table::has) && !named.contains(rule.window().tsColumn())) {
        named.add(rule.window().tsColumn());
      }
    }
    if (named.size() > 1) {
      LOG.warn("window rules on {} count in different time columns {}; the scan is ordered by {} "
              + "and events of the others may arrive out of order — watch "
              + "kahshe_watch_window_late_drops_total", table.name(), named, named.get(0));
    }
    return named.isEmpty() ? null : named.get(0);
  }

  /**
   * The longest timeframe any window rule on this table asks for: what must be replayed to
   * rebuild the partial windows a restart lost. Nothing older can be part of a live window.
   */
  @Override
  public long replayMs(TableView table) {
    WatchRules loaded = rules;
    if (loaded == null) {
      return 0;
    }
    long longest = 0;
    for (WatchRule rule : loaded.current()) {
      if (rule.window() == null) {
        continue;
      }
      List<String> needed = new ArrayList<>(rule.columns());
      needed.addAll(rule.window().columns());
      if (needed.stream().allMatch(table::has)) {
        longest = Math.max(longest, rule.window().timeframeMs());
      }
    }
    return longest;
  }

  @Override
  public FileScan open(FileScanContext ctx) {
    WatchRules loaded = rules;
    Alerts sink = alerts;
    if (loaded == null || sink == null) {
      return null;
    }
    String qualified = ctx.namespace() + "." + ctx.tableName();
    List<Windowed> windowed = new ArrayList<>();
    for (WatchRule rule : loaded.current()) {
      if (rule.window() == null
          || !rule.prefix().equals(ctx.prefix())
          || !rule.table().equals(qualified)) {
        continue;
      }
      List<String> needed = new ArrayList<>(rule.columns());
      needed.addAll(rule.window().columns());
      if (!needed.stream().allMatch(c -> ctx.kinds().containsKey(c))) {
        continue;
      }
      PreparedRule matcher;
      try {
        matcher = PreparedRule.of(rule, ctx.kinds(), ctx.repeated());
      } catch (PreparedRule.UnsupportedRule e) {
        // Skipped rather than fatal, as on the row-scan path: a window rule this evaluator cannot
        // answer must not stop the ones it can.
        LOG.warn("watch rule {} skipped on {}: {}", rule.id(), ctx.tableName(), e.getMessage());
        continue;
      }
      WindowCounters counters =
          byRule.computeIfAbsent(
              rule.id(),
              id -> new WindowCounters(
                  rule.window().count(), rule.window().timeframeMs(), maxKeys, metrics));
      windowed.add(new Windowed(matcher, counters,
          ctx.kinds().getOrDefault(rule.window().tsColumn(), ValueKind.OTHER)));
    }
    return windowed.isEmpty() ? null : new Scan(windowed, ctx, sink);
  }

  /** One window rule ready for a file: its matcher, its counters, its time column's type. */
  private record Windowed(PreparedRule prepared, WindowCounters counters, ValueKind tsKind) {}

  /** One file's contribution to every window rule on this table. */
  private static final class Scan implements FileScan {
    private final List<Windowed> rules;
    private final FileScanContext ctx;
    private final Alerts alerts;
    private final List<Map<String, Object>> raised = new ArrayList<>();
    private final boolean[][] hits;
    private int rowCount;

    Scan(List<Windowed> rules, FileScanContext ctx, Alerts alerts) {
      this.rules = rules;
      this.ctx = ctx;
      this.alerts = alerts;
      this.hits = new boolean[rules.size()][];
      for (int i = 0; i < rules.size(); i++) {
        hits[i] = new boolean[rules.get(i).prepared().fieldCount()];
      }
    }

    @Override
    public void row(int rowPosition, Row row) {
      rowCount = rowPosition + 1;
      for (int i = 0; i < rules.size(); i++) {
        Windowed windowed = rules.get(i);
        WatchRule rule = windowed.prepared().rule();
        try {
          if (!windowed.prepared().matches(row, hits[i])) {
            continue;
          }
          long ts = epochMillis(row, rule.window().tsColumn(), windowed.tsKind());
          if (ts == Long.MIN_VALUE) {
            continue; // a time that cannot be read cannot be placed in a window
          }
          String key = key(row, rule.window());
          WindowCounters.Trip trip = windowed.counters().offer(key, ts);
          if (trip != null) {
            raised.add(payload(windowed, key, trip, rowPosition));
          }
        } catch (RuntimeException e) {
          LOG.warn("window rule {} failed on row {} of {}", rule.id(), rowPosition, ctx.path(), e);
        }
      }
    }

    @Override
    public List<Map<String, Object>> finish() {
      return raised;
    }

    /** The group key: one column's value, or the values joined; a null value is its own group. */
    private static String key(Row row, WatchRule.Window window) {
      List<String> groupBy = window.groupBy();
      if (groupBy.isEmpty()) {
        return UNGROUPED;
      }
      if (groupBy.size() == 1) {
        String value = row.canonical(groupBy.get(0));
        return value == null ? "" : value;
      }
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < groupBy.size(); i++) {
        if (i > 0) {
          sb.append('\u001f'); // unit separator: not a character a canonical value carries
        }
        String value = row.canonical(groupBy.get(i));
        sb.append(value == null ? "" : value);
      }
      return sb.toString();
    }

    private Map<String, Object> payload(
        Windowed windowed, String key, WindowCounters.Trip trip, int rowPosition) {
      WatchRule rule = windowed.prepared().rule();
      WatchRule.Window window = rule.window();
      Map<String, Object> evidence = new LinkedHashMap<>();
      evidence.put("kind", "window");
      evidence.put("column", window.groupBy().isEmpty() ? window.tsColumn() : window.groupBy());
      evidence.put("values", List.of(key));
      evidence.put("count", window.count());
      evidence.put("row_positions", List.of(rowPosition));
      // exact when the snapshot allows it: the rows were read, but on a delete-bearing snapshot
      // any count is an upper bound
      evidence.put("confidence", ctx.confidence());
      Map<String, Object> extra = new LinkedHashMap<>();
      extra.put("matched_rows", window.count());
      extra.put("window_start_ms", trip.startMs());
      extra.put("window_end_ms", trip.endMs());
      extra.put("window_timeframe_ms", window.timeframeMs());
      extra.put("window_key", key);
      if (ctx.replay()) {
        // Rebuilt from files this process is seeing for the first time but the last one may not
        // have been: this window may already have alerted before the restart. A receiver dedupes
        // on (rule, window_key, window_end_ms), which the payload carries for exactly that.
        extra.put("replayed", true);
      }
      // What tells two keys tripping in one file apart; the pass strips it before delivery.
      extra.put(ScanPass.CLAIM_KEY, ctx.path() + "|" + rule.id() + "|" + key + "|" + trip.endMs());
      String sql = ConfirmationSql.window(alerts.sqlCatalog(), ctx.namespace(), ctx.tableName(),
          ctx.snapshotId(), rule, windowed.prepared().numericColumns(), windowed.tsKind(),
          trip.startMs(), trip.endMs());
      LOG.warn("WATCH ALERT rule={} severity={} table={}.{} window={}..{} key={} count={} file={}",
          rule.id(), rule.severity().name().toLowerCase(Locale.ROOT), ctx.namespace(),
          ctx.tableName(), trip.startMs(), trip.endMs(), key, window.count(), ctx.path());
      return alerts.payload(rule, ctx.prefix(), ctx.namespace(), ctx.tableName(), ctx.snapshotId(),
          ctx.path(), rowCount, List.of(evidence), extra, "window", false, sql);
    }
  }

  /**
   * The row's event time as epoch milliseconds, or {@link Long#MIN_VALUE} when it cannot be read.
   *
   * <p>A timestamp is microseconds and a date is days, so the unit comes from the column's KIND
   * rather than from the value's magnitude — guessing from magnitude is how a seconds column
   * silently becomes 1970.
   *
   * <p>The generic data model hands back {@code LocalDateTime}, {@code OffsetDateTime} or
   * {@code LocalDate} for the temporal types, and Iceberg's internal one hands back the number.
   * Both are read here: the temporal objects DIRECTLY rather than through their text, because
   * this runs per matching row and formatting then parsing a string is real work to arrive back
   * where we started.
   */
  static long epochMillis(Row row, String column, ValueKind kind) {
    Object value = row.value(column);
    if (value == null) {
      return Long.MIN_VALUE;
    }
    if (value instanceof java.time.OffsetDateTime odt) {
      return odt.toInstant().toEpochMilli();
    }
    if (value instanceof java.time.LocalDateTime ldt) {
      return ldt.toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
    }
    if (value instanceof java.time.Instant instant) {
      return instant.toEpochMilli();
    }
    if (value instanceof java.time.LocalDate date) {
      return date.toEpochDay() * 86_400_000L;
    }
    return switch (kind) {
      case TIMESTAMP, TIMESTAMPTZ -> value instanceof Number n
          ? n.longValue() / 1000
          : parseInstant(String.valueOf(value));
      case DATE -> value instanceof Number n ? n.longValue() * 86_400_000L : Long.MIN_VALUE;
      case INTEGRAL -> value instanceof Number n ? n.longValue() : Long.MIN_VALUE;
      case STRING -> parseInstant(String.valueOf(value));
      default -> Long.MIN_VALUE;
    };
  }

  private static long parseInstant(String text) {
    try {
      return java.time.Instant.parse(text).toEpochMilli();
    } catch (java.time.format.DateTimeParseException e) {
      try {
        return java.time.LocalDateTime.parse(text)
            .toInstant(java.time.ZoneOffset.UTC)
            .toEpochMilli();
      } catch (java.time.format.DateTimeParseException ignored) {
        return Long.MIN_VALUE;
      }
    }
  }
}
