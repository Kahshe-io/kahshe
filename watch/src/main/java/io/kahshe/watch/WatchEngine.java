package io.kahshe.watch;

import io.kahshe.common.Metrics;
import io.kahshe.indexer.build.IndexBuildListener;
import io.kahshe.format.type.gram.Grams;
import io.kahshe.format.type.term.TermIndexWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.kahshe.watch.rules.ConfirmationSql;
import io.kahshe.watch.rules.WatchRule;
import io.kahshe.watch.rules.WatchRules;
import io.kahshe.watch.sink.AlertSink;

/**
 * Rule evaluation riding index builds: each file's term counts and gram set decide
 * {@code match} and {@code contains} on one indexed column, with no data read of its own. Rules
 * of any other shape belong to {@link io.kahshe.watch.scan.ScanPass}; this engine keeps only the
 * ones {@link WatchRule#ridesIndex} admits, and both paths claim through the shared
 * {@link Alerts}, so a rule both can evaluate still alerts once per file.
 *
 * <p>Delivery is at-least-once: the (rule id, file path) claim is a bounded in-memory cache, so
 * eviction, a restart, or a compaction that rewrites file paths can re-alert. Full rebuilds of
 * previously covered files are suppressed and counted unless
 * KAHSHE_WATCH_REALERT_ON_REBUILD=true.
 *
 * <p>The counts this rides are as delete-unaware as {@link io.kahshe.watch.scan.ScanPass}'s rows:
 * {@code IndexBuilder.readFile} opens each data file's raw Parquet and applies no delete file, so
 * on a merge-on-read snapshot a term count includes rows the engine no longer returns. The
 * evidence therefore says {@code exact} only over a snapshot PROVEN delete-free.
 */
public final class WatchEngine implements IndexBuildListener {
  private static final Logger LOG = LoggerFactory.getLogger(WatchEngine.class);

  private final WatchRules rules;
  private final Alerts alerts;
  private final Metrics metrics;
  private final boolean realertOnRebuild;
  private final Deletes deletes;

  /**
   * Whether the snapshot a build is riding carries delete files — the one thing this path needs
   * to know and cannot find out for itself.
   *
   * <p>{@link IndexBuildListener#start} hands a listener a snapshot ID and nothing else, and
   * PortBoundaryTest forbids this class the table format it would take to resolve one, which is
   * the right refusal: reading a snapshot summary here would make the evaluation path Iceberg's.
   * So the verdict arrives as a boolean from whoever already holds the table, on the same
   * fail-closed rule the plan path and {@code ScanPass} use — {@code total-delete-files} that
   * does not PROVE zero counts as delete-bearing.
   */
  @FunctionalInterface
  public interface Deletes {
    /** True unless the snapshot is proven to carry no delete files. */
    boolean bearing(String prefix, String namespace, String tableName, long snapshotId);

    /** What an engine wired without one answers: nothing is proven, so nothing is exact. */
    Deletes UNPROVEN = (prefix, namespace, tableName, snapshotId) -> true;
  }

  /**
   * True only when some loaded rule matches on tokens: producing counts costs the index builder a
   * per-file map of every distinct term in the file, while {@code contains} evidence is probed
   * against the 3-gram set the build produces regardless. Re-read per call rather than cached,
   * because rules hot-reload: a config edited to add token rules must start paying for counts
   * again rather than silently never firing.
   */
  @Override
  public boolean readsTermCounts() {
    return rules.current().stream().anyMatch(rule -> !rule.match().isEmpty());
  }

  public WatchEngine(WatchRules rules, AlertSink sink, Metrics metrics,
      boolean realertOnRebuild, String sqlCatalog) {
    this(rules, sink, metrics, realertOnRebuild, sqlCatalog, null);
  }

  /** As above, told whether the snapshot each build rides carries delete files. */
  public WatchEngine(WatchRules rules, AlertSink sink, Metrics metrics,
      boolean realertOnRebuild, String sqlCatalog, Deletes deletes) {
    this(rules, new Alerts(sink, metrics, sqlCatalog), metrics, realertOnRebuild, deletes);
  }

  /**
   * As above, over shared alert state. One {@link Alerts} is handed to this engine and to
   * {@link io.kahshe.watch.scan.ScanPass}, so a rule both paths can evaluate alerts once per file.
   */
  public WatchEngine(WatchRules rules, Alerts alerts, Metrics metrics, boolean realertOnRebuild) {
    this(rules, alerts, metrics, realertOnRebuild, null);
  }

  /** As above, with the {@link Deletes} verdict; null leaves every snapshot unproven. */
  public WatchEngine(WatchRules rules, Alerts alerts, Metrics metrics, boolean realertOnRebuild,
      Deletes deletes) {
    this.rules = rules;
    this.alerts = alerts;
    this.metrics = metrics;
    this.realertOnRebuild = realertOnRebuild;
    this.deletes = deletes == null ? Deletes.UNPROVEN : deletes;
  }

  /**
   * The verdict, failing closed: a resolver that throws has proven nothing, so the snapshot is
   * delete-bearing.
   */
  private boolean deleteBearing(String prefix, String namespace, String tableName,
      long snapshotId) {
    try {
      return deletes.bearing(prefix, namespace, tableName, snapshotId);
    } catch (RuntimeException e) {
      LOG.warn("could not establish whether snapshot {} of {}.{} carries delete files; evidence "
          + "from this build is labelled advisory rather than exact", snapshotId, namespace,
          tableName, e);
      return true;
    }
  }

  @Override
  public BuildContext start(String prefix, String namespace, String tableName, String column,
      long snapshotId, BuildKind buildKind, Set<String> priorCovered, Grams.Contract grams) {
    String qualified = namespace + "." + tableName;
    // ridesIndex(): the shapes whose file-level answer is their row-level answer; anything else is
    // the row scan's, never half-evaluated here.
    List<WatchRule> matched =
        rules.current().stream()
            .filter(r -> r.prefix().equals(prefix)
                && r.table().equals(qualified)
                && r.ridesIndex()
                && column.equals(r.column()))
            .toList();
    if (matched.isEmpty()) {
      return NOOP_CONTEXT;
    }
    // Asked once per build, not per file: the resolver may reach a catalog, and the snapshot is
    // fixed for the life of the build.
    boolean deleteBearing = deleteBearing(prefix, namespace, tableName, snapshotId);
    // Only a token (match) rule reads counts; contains evidence is probed against the gram set and
    // is already labelled advisory, so warning a contains-only rule set names a bound it does not
    // have.
    boolean anyTokenRule = matched.stream().anyMatch(r -> !r.match().isEmpty());
    if (deleteBearing && anyTokenRule) {
      metrics.watchBuildDeleteBearing.increment();
      LOG.warn("watch rules riding the build of {}.{} cannot prove snapshot {} is delete-free, so "
              + "token counts are UPPER BOUNDS: the build reads raw data files and applies no "
              + "delete file. Evidence says advisory, and the confirmation SQL — which does apply "
              + "the deletes — may return fewer rows than the alert claimed.",
          qualified, column, snapshotId);
    }
    return new Context(matched, prefix, namespace, tableName, column, snapshotId, buildKind,
        priorCovered, grams, deleteBearing);
  }

  private final class Context implements BuildContext {
    private final List<Map<String, Object>> raised = new java.util.ArrayList<>();
    private final Grams.Contract gramRule;
    private final List<WatchRule> matched;
    private final String prefix;
    private final String namespace;
    private final String tableName;
    private final String column;
    private final long snapshotId;
    private final BuildKind buildKind;
    private final Set<String> priorCovered;
    /** What token evidence from this build may honestly claim; see {@link #deleteBearing}. */
    private final String confidence;

    Context(List<WatchRule> matched, String prefix, String namespace, String tableName,
        String column, long snapshotId, BuildKind buildKind, Set<String> priorCovered,
        Grams.Contract gramRule, boolean deleteBearing) {
      this.confidence = deleteBearing ? "advisory" : "exact";
      this.gramRule = gramRule;
      this.matched = matched;
      this.prefix = prefix;
      this.namespace = namespace;
      this.tableName = tableName;
      this.column = column;
      this.snapshotId = snapshotId;
      this.buildKind = buildKind;
      this.priorCovered = priorCovered;
    }

    @Override
    public void file(String path, TermIndexWriter.FileTerms terms, Set<String> grams) {
      boolean rebuildOfCovered = buildKind == BuildKind.FULL && priorCovered.contains(path);
      // Counts that hit the per-file distinct-term cap are a lower bound, so a min_count rule
      // reading them could quietly fail to fire: token rules are skipped for that file, counted
      // and logged rather than failing the build. contains rules read the uncapped gram set and
      // are unaffected. A null terms is deliberately NOT dereferenced here — this runs outside
      // the per-rule catch below, and a broken file is that catch's to report, not a capped one.
      boolean countsUsable = terms == null || !terms.countsTruncated();
      if (!countsUsable) {
        metrics.watchCountsTruncated.increment();
        LOG.warn(
            "term counts for {} hit the per-file cap; token (match) rules are SKIPPED for this "
                + "file and may not fire. contains rules are unaffected. Raise "
                + "kahshe.watch.max.distinct.terms if this file is legitimately that diverse.",
            path);
      }
      for (WatchRule rule : matched) {
        if (!countsUsable && !rule.match().isEmpty()) {
          continue;
        }
        try {
          evaluate(rule, path, terms, grams, rebuildOfCovered);
        } catch (RuntimeException e) {
          // a poisoned rule must not take down the others (or the build)
          LOG.warn("watch rule {} failed on {}; skipped for this file", rule.id(), path, e);
        }
      }
    }

    @Override
    public void done() {}

    @Override
    public List<Map<String, Object>> alerts() {
      return List.copyOf(raised);
    }

    private void evaluate(WatchRule rule, String path, TermIndexWriter.FileTerms terms,
        Set<String> grams, boolean rebuildOfCovered) {
      List<Map<String, Object>> evidence = new ArrayList<>();
      long tokenTotal = 0;
      // One verdict per FIELD, then the rule's tree through the same interpreter the row scan
      // uses, so the two paths cannot disagree: a field's values are OR'ed by the record's
      // contract, so `match: [a, b]` under all-of is ONE field, satisfied by either token.
      List<WatchRule.Field> fields = rule.where();
      boolean[] hits = new boolean[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        WatchRule.Field field = fields.get(i);
        for (String value : field.values()) {
          if (field.op() == WatchRule.Op.MATCH) {
            long count = terms.countOf(value);
            tokenTotal += count;
            if (count > 0) {
              hits[i] = true;
              Map<String, Object> e = new LinkedHashMap<>();
              e.put("kind", "token");
              e.put("value", value);
              e.put("count", count);
              e.put("confidence", confidence);
              evidence.add(e);
            }
          } else if (grams.containsAll(gramRule.gramsOf(value))) {
            hits[i] = true;
            Map<String, Object> e = new LinkedHashMap<>();
            e.put("kind", "contains");
            e.put("value", value);
            e.put("confidence", "advisory");
            evidence.add(e);
          }
        }
      }
      boolean fires = io.kahshe.watch.rules.Conditions.eval(rule.expr(), hits);
      // min_count is a token-occurrence threshold on top of the condition; loader guarantees
      // that min_count > 1 implies the rule declares match tokens. On a delete-bearing snapshot
      // tokenTotal is an upper bound, so the threshold can be crossed by occurrences a delete file
      // already removed — the rule still fires, and `confidence` above is what tells the analyst
      // why the confirmation may come back short of it.
      if (fires && rule.minCount() > 1 && tokenTotal < rule.minCount()) {
        fires = false;
      }
      if (!fires) {
        return;
      }
      if (rebuildOfCovered && !realertOnRebuild) {
        metrics.watchSuppressed.increment();
        return;
      }
      if (!alerts.claim(rule.id(), path)) {
        return;
      }

      List<String> literals = new ArrayList<>();
      for (Map<String, Object> e : evidence) {
        literals.add((String) e.get("value"));
      }
      // The tree overload, not the literals one: the SQL must ask the rule as written — every
      // field, with the tree's own connectives — or it confirms something other than the rule.
      String sql = ConfirmationSql.build(alerts.sqlCatalog(), namespace, tableName, snapshotId,
          path, fields, Set.of(), rule.expr());
      String kind = buildKind.name().toLowerCase(Locale.ROOT);
      LOG.warn("WATCH ALERT rule={} severity={} prefix={} table={}.{} column={} file={} "
              + "snapshot={} build_kind={} evidence={}",
          rule.id(), rule.severity().name().toLowerCase(Locale.ROOT), prefix, namespace,
          tableName, column, path, snapshotId, kind, literals);

      Map<String, Object> payload = alerts.payload(rule, prefix, namespace, tableName, snapshotId,
          path, terms.rowCount(), evidence, Map.of(), kind, rebuildOfCovered, sql);
      raised.add(payload);
      alerts.deliver(rule.id(), payload);
    }
  }
}
