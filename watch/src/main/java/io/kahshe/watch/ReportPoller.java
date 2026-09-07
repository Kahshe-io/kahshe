package io.kahshe.watch;

import io.kahshe.common.Metrics;
import io.kahshe.format.BuildReport;
import io.kahshe.format.FormatConfig;
import io.kahshe.format.IndexPaths;
import io.kahshe.indexer.TableSource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.kahshe.watch.rules.WatchRule;
import io.kahshe.watch.rules.WatchRules;

/**
 * Alerting out of the builder's process: on the discovery interval, reads each watched column's
 * build report (docs/FORMAT.md section 12) and, when a build it has not seen has landed, delivers
 * the alerts that build raised. It evaluates nothing itself: this is delivery, what a process
 * with the indexer off runs beside its own row scan. Reports chain by id: a report whose
 * predecessor is not the one last seen means a build went unreported here, which is counted and
 * logged.
 *
 * <p>Delivery goes through the same {@link Alerts} as the in-process evaluators, under the same
 * (rule, file) claim: a file this process has already alerted on — the row scan reached it, or an
 * earlier report carried it — is not alerted on again from a report.
 */
public final class ReportPoller {
  private static final Logger LOG = LoggerFactory.getLogger(ReportPoller.class);
  private final WatchRules rules;
  private final TableSource catalogs;
  private final FormatConfig format;
  private final WatchConfig config;
  private final Alerts alerts;
  private final Metrics metrics;
  private final Map<String, String> lastSeen = new ConcurrentHashMap<>();

  public ReportPoller(WatchRules rules, TableSource catalogs, FormatConfig format, WatchConfig config,
      Alerts alerts, Metrics metrics) {
    this.rules = rules;
    this.catalogs = catalogs;
    this.format = format;
    this.config = config;
    this.alerts = alerts;
    this.metrics = metrics;
  }

  public void start() {
    Thread poller = new Thread(this::loop, "kahshe-watch-reports");
    poller.setDaemon(true);
    poller.start();
  }

  private void loop() {
    while (true) {
      try {
        pollOnce();
        Thread.sleep(config.watchPollMs());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      } catch (RuntimeException e) {
        LOG.warn("report poll failed; retrying next interval", e);
      }
    }
  }

  /** {@code alert[outer][inner]} as text; the payload shape is pinned by the report schema. */
  private static String nested(Map<String, Object> alert, String outer, String inner) {
    return alert.get(outer) instanceof Map<?, ?> m ? String.valueOf(m.get(inner)) : "?";
  }

  /** One pass over every watched (prefix, table, column). */
  public void pollOnce() {
    Map<String, List<WatchRule>> byTable = WatchRules.byTable(rules.current());
    for (Map.Entry<String, List<WatchRule>> entry : byTable.entrySet()) {
      WatchRule first = entry.getValue().get(0);
      try {
        TableIdentifier ident = TableIdentifier.parse(first.table());
        Table table = catalogs.load(first.prefix(), ident);
        String root = IndexPaths.root(table, format.indexRoot());
        // Every column any rule names, not rule.column(): a rule spanning columns has no single
        // column, and reading one here would take the whole table's poll down with it.
        List<String> columns =
            entry.getValue().stream().flatMap(r -> r.columns().stream()).distinct().toList();
        for (String column : columns) {
          Types.NestedField field = table.schema().findField(column);
          if (field == null) {
            continue;
          }
          BuildReport report = BuildReport.read(IndexPaths.io(table, format), root, field.fieldId());
          if (report == null) {
            continue;
          }
          String key = entry.getKey() + "|" + column;
          String seen = lastSeen.get(key);
          if (report.buildId().equals(seen)) {
            continue;
          }
          if (seen != null && !seen.equals(report.previousBuildId())) {
            metrics.watchReportsMissed.increment();
            LOG.warn("build report for {} {} chains to {} but the last one seen here was {}: a build "
                + "went unreported to this watcher", first.table(), column, report.previousBuildId(), seen);
          }
          for (Map<String, Object> alert : report.alerts()) {
            String ruleId = nested(alert, "rule", "id");
            if (!alerts.claim(ruleId, nested(alert, "file", "path"))) {
              continue;
            }
            alerts.deliver(ruleId, alert);
            metrics.watchReportAlertsDelivered.increment();
          }
          metrics.watchReportsSeen.increment();
          lastSeen.put(key, report.buildId());
          LOG.info("build report {} for {} {}: {} ({} alert(s), {} file(s) added)", report.buildId(),
              first.table(), column, report.kind(), report.alerts().size(), report.added().size());
        }
      } catch (Exception e) {
        LOG.warn("report poll failed for {} table {}", first.prefix(), first.table(), e);
      }
    }
  }
}
