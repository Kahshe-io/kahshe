package io.kahshe.watch;

import io.kahshe.common.Metrics;
import io.kahshe.indexer.maintain.Fleet;
import io.kahshe.indexer.maintain.IndexerService;
import io.kahshe.indexer.TableSource;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.kahshe.watch.rules.WatchRule;
import io.kahshe.watch.rules.WatchRules;
import io.kahshe.watch.scan.ScanPass;

/**
 * Keeps watched tables' indexes fresh without relying on proxy traffic: a daemon poll loop over
 * only the distinct (prefix, table) pairs named in loaded rules — never a namespace listing (the
 * backend catalog wrapper does not support namespace operations). Each poll loads the table,
 * reads the {@code kahshe.index} property and current snapshot, and feeds the indexer's observe
 * path.
 *
 * <p>Only the index observation is gated on {@code kahshe.index}; the row {@link ScanPass} is
 * handed every polled table, because what it reads is decided by the rules, not by the index. A
 * rule is therefore uncovered only when the table's schema has no such column, which is gauged
 * and logged (rate-limited) naming the column.
 */
public final class TableDiscovery {
  private static final Logger LOG = LoggerFactory.getLogger(TableDiscovery.class);
  private static final long UNCOVERED_LOG_INTERVAL_MS = 10 * 60 * 1000;

  private final WatchRules rules;
  private final TableSource catalogs;
  private final IndexerService indexer;
  private final WatchConfig config;
  private final ScanPass scan;
  private final Map<String, Long> lastUncoveredLog = new ConcurrentHashMap<>();
  private volatile long uncovered;

  /** Discovery with no row scan: KAHSHE_WATCH_SCAN=false. */
  public TableDiscovery(WatchRules rules, TableSource catalogs, IndexerService indexer,
      WatchConfig config, Metrics metrics) {
    this(rules, catalogs, indexer, config, metrics, null);
  }

  /** As above, handing every polled table to the row scan as well. */
  public TableDiscovery(WatchRules rules, TableSource catalogs, IndexerService indexer,
      WatchConfig config, Metrics metrics, ScanPass scan) {
    this.rules = rules;
    this.catalogs = catalogs;
    this.indexer = indexer;
    this.config = config;
    this.scan = scan;
    metrics.watchRulesUncovered = () -> uncovered;
  }

  public void start() {
    Thread poller = new Thread(this::loop, "kahshe-watch-discovery");
    poller.setDaemon(true);
    poller.start();
  }

  private void loop() {
    while (true) {
      try {
        pollOnce();
      } catch (Throwable e) {
        // Throwable: an Error here would silently end the sole discovery thread
        LOG.warn("watch discovery poll failed", e);
      }
      try {
        Thread.sleep(config.watchPollMs());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  public void pollOnce() {
    Map<String, List<WatchRule>> byTable = WatchRules.byTable(rules.current());
    long uncoveredCount = 0;
    // Rotated by this process's fleet ordinal so N replicas start on N different tables; outside
    // a fleet the ordinal is 0 and the order is the rules' own. Every table is still visited each
    // poll, so the scan sees them all.
    for (Map.Entry<String, List<WatchRule>> entry :
        Fleet.rotate(List.copyOf(byTable.entrySet()), indexer.fleetOrdinal())) {
      WatchRule first = entry.getValue().get(0);
      String prefix = first.prefix();
      try {
        TableIdentifier ident = TableIdentifier.parse(first.table());
        Table table = catalogs.load(prefix, ident);
        List<String> indexed = indexedColumns(table.properties().get("kahshe.index"));
        for (WatchRule rule : entry.getValue()) {
          // The schema, not kahshe.index: the row scan reads whatever a rule names, so the only
          // rule that cannot fire anywhere is one naming a column the table does not have.
          String missing = missingColumn(table, rule);
          if (missing != null) {
            uncoveredCount++;
            long now = System.currentTimeMillis();
            Long last = lastUncoveredLog.get(rule.id());
            if (last == null || now - last >= UNCOVERED_LOG_INTERVAL_MS) {
              lastUncoveredLog.put(rule.id(), now);
              LOG.warn("watch rule {} cannot fire: table {} has no column {} in its schema",
                  rule.id(), first.table(), missing);
            }
          }
        }
        long snapshotId =
            table.currentSnapshot() == null ? -1 : table.currentSnapshot().snapshotId();
        // same gate as the loadTable passthrough: a table earns observation by declaring indexed
        // columns. The row scan below is not gated on it.
        if (!indexed.isEmpty() && snapshotId > 0) {
          String[] raw = encodeForObserve(prefix, ident);
          indexer.observe(raw[0], raw[1], raw[2], indexed, snapshotId);
        }
        if (scan != null) {
          scan.scan(prefix, ident, table);
        }
      } catch (Throwable e) {
        LOG.warn("watch discovery failed for {} table {}", prefix, first.table(), e);
      }
    }
    uncovered = uncoveredCount;
  }

  /**
   * {@link IndexerService#observe} takes raw url-encoded segments (its worker applies
   * {@link IndexerService#decodeIdent}); this is the matching encode.
   */
  public static String[] encodeForObserve(String prefix, TableIdentifier ident) {
    return new String[] {
      URLEncoder.encode(prefix, StandardCharsets.UTF_8),
      RESTUtil.encodeNamespace(ident.namespace(), IndexerService.NAMESPACE_SEPARATOR),
      URLEncoder.encode(ident.name(), StandardCharsets.UTF_8)
    };
  }

  /** The first column this rule names that the table's schema lacks, or null when all are there. */
  private static String missingColumn(Table table, WatchRule rule) {
    for (String column : rule.columns()) {
      if (table.schema() == null || table.schema().findField(column) == null) {
        return column;
      }
    }
    return null;
  }

  private static List<String> indexedColumns(String property) {
    if (property == null || property.isBlank()) {
      return List.of();
    }
    List<String> columns = new ArrayList<>();
    for (String column : property.split(",")) {
      if (!column.isBlank()) {
        columns.add(column.trim());
      }
    }
    return columns;
  }
}
