package io.kahshe.watch;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kahshe.common.BoundedCache;
import io.kahshe.common.Metrics;
import io.kahshe.watch.rules.WatchRule;
import io.kahshe.watch.sink.AlertSink;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The one alert payload and the one suppression state, shared by every evaluator.
 *
 * <p>{@link WatchEngine} evaluates per file while an index build reads it, and
 * {@link io.kahshe.watch.scan.ScanPass} per row over the columns its scanners name. A
 * single-column rule on an indexed column is seen by both, so the (rule id, file path) claim and
 * the payload shape live here rather than in either of them.
 *
 * <p>Delivery is at-least-once: the claim is a bounded in-memory cache, so eviction, a restart, or
 * a compaction that rewrites file paths can re-alert.
 */
public final class Alerts {
  private static final Logger LOG = LoggerFactory.getLogger(Alerts.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final AlertSink sink;
  private final Metrics metrics;
  private final String sqlCatalog;
  private final BoundedCache<String, Boolean> fired = new BoundedCache<>(65536);

  public Alerts(AlertSink sink, Metrics metrics, String sqlCatalog) {
    this.sink = sink;
    this.metrics = metrics;
    this.sqlCatalog = sqlCatalog;
  }

  /** The catalog name the confirmation SQL is written against. */
  public String sqlCatalog() {
    return sqlCatalog;
  }

  /**
   * Claims (rule, file) for this process: true the first time, false ever after. The caller
   * alerts only on true, so a file reached by both evaluators alerts once.
   */
  public synchronized boolean claim(String ruleId, String path) {
    String key = ruleId + "|" + path;
    if (fired.get(key) != null) {
      return false;
    }
    fired.put(key, Boolean.TRUE);
    return true;
  }

  /** The alert JSON, as every sink and every build report carries it. */
  public Map<String, Object> payload(
      WatchRule rule,
      String prefix,
      String namespace,
      String tableName,
      long snapshotId,
      String path,
      long rowCount,
      List<Map<String, Object>> evidence,
      Map<String, Object> extra,
      String kind,
      boolean reObservation,
      String sql) {
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("rule", Map.of(
        "id", rule.id(),
        "title", rule.title(),
        "severity", rule.severity().name().toLowerCase(Locale.ROOT)));
    Map<String, Object> table = new LinkedHashMap<>();
    table.put("prefix", prefix);
    table.put("namespace", namespace);
    table.put("table", tableName);
    table.put("snapshot_id", snapshotId);
    payload.put("table", table);
    Map<String, Object> file = new LinkedHashMap<>();
    file.put("path", path);
    file.put("row_count", rowCount);
    payload.put("file", file);
    payload.put("evidence", evidence);
    // What only a row-level evaluator knows — how many rows satisfied the whole rule, and where.
    // The file-level path has nothing to say here and passes an empty map.
    payload.putAll(extra);
    payload.put("build_kind", kind);
    payload.put("re_observation", reObservation);
    payload.put("confirmation_sql", sql);
    payload.put("emitted_at_ms", System.currentTimeMillis());
    return payload;
  }

  /** Counts the alert and hands it to the sink; a serialization failure never propagates. */
  public void deliver(String ruleId, Map<String, Object> payload) {
    metrics.watchAlerts.increment();
    try {
      sink.deliver(MAPPER.writeValueAsBytes(payload));
    } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
      LOG.warn("alert payload serialization failed for rule {}", ruleId, e);
    }
  }
}
