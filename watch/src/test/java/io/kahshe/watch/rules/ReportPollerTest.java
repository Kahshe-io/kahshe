package io.kahshe.watch.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kahshe.watch.Alerts;

import io.kahshe.common.Metrics;
import io.kahshe.common.Records;
import io.kahshe.format.BuildReport;
import io.kahshe.format.FormatConfig;
import io.kahshe.format.IndexPaths;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.indexer.TableSource;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.watch.RecordingSink;
import io.kahshe.watch.ReportPoller;
import io.kahshe.watch.WatchConfig;

/**
 * The out-of-process half of alerting: the poller acts once per build report it has not seen,
 * delivers the alerts that report carries, and counts a report whose predecessor is not the one
 * it last saw.
 */
class ReportPollerTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  @TempDir Path tmp;

  /** A report of {@code alerts} alerts, rules r0..rN, all on the file {@code path}. */
  private static BuildReport report(String id, String previous, int fieldId, int alerts, String path) {
    List<Map<String, Object>> raised = new java.util.ArrayList<>();
    for (int i = 0; i < alerts; i++) {
      raised.add(Map.of("rule", Map.of("id", "r" + i), "file", Map.of("path", path)));
    }
    return new BuildReport(id, previous, "p", "logs", "events", LocalTableFixture.COLUMN, fieldId, 7L,
        BuildReport.Kind.INCREMENTAL, "kahshe-ascii-v3-max256", "kahshe-grams-v2-n3", 1L, 2L,
        Map.of("files-covered", 1L, "files-added", 1L, "files-departed", 0L), List.of("f"), List.of(),
        List.of(), List.of(), raised);
  }

  private static TableSource source(Table table) {
    return new TableSource() {
      @Override
      public Table load(String prefix, TableIdentifier ident) {
        return table;
      }

      @Override
      public void invalidate(String prefix, TableIdentifier ident) {}
    };
  }

  /** A rule spanning two columns — the shape that has no single {@code column()}. */
  private static WatchRule crossColumn() {
    return new WatchRule("r-cross", "t", WatchRule.Severity.LOW, "p", "logs.events",
        List.of(new WatchRule.Field(LocalTableFixture.COLUMN, WatchRule.Op.EQUALS, List.of("alpha")),
            new WatchRule.Field("other", WatchRule.Op.EQUALS, List.of("x"))),
        WatchRule.Condition.ALL_OF, 1);
  }

  private static WatchRules rules(Metrics metrics) {
    WatchRule rule = WatchRule.singleColumn("r0", "t", WatchRule.Severity.LOW, "p", "logs.events",
        LocalTableFixture.COLUMN, List.of("alpha"), List.of(), WatchRule.Condition.ANY_OF, 1);
    return WatchRules.fixed(List.of(rule), metrics);
  }

  private static final WatchConfig CONFIG =
      new WatchConfig("", "webhook", "", "", 1_000, 60_000, "iceberg", false, true, 2);

  // Verified red with the chain check removed: the missed count stayed 0.
  @Test
  void deliversOncePerNewReportAndCountsAMissedOne() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha");
    BuildConfig build = LocalTableFixture.config();
    FormatConfig format = Records.with(build.format(), Map.of("indexRoot", "file:" + tmp.resolve("index")));
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    String root = IndexPaths.root(table, format.indexRoot());
    Metrics metrics = new Metrics();
    RecordingSink sink = new RecordingSink();
    ReportPoller poller = new ReportPoller(rules(metrics), source(table), format, CONFIG,
        new Alerts(sink, metrics, "iceberg"), metrics);

    poller.pollOnce();
    assertEquals(0, metrics.watchReportsSeen.sum(), "no report yet: nothing to act on");

    report("b1", null, fieldId, 2, "f").write(table.io(), root);
    poller.pollOnce();
    poller.pollOnce();
    assertEquals(1, metrics.watchReportsSeen.sum(), "once per build, however often it polls");
    assertEquals(2, metrics.watchReportAlertsDelivered.sum(), "the alerts the build raised");
    assertEquals(0, metrics.watchReportsMissed.sum());

    report("b2", "b1", fieldId, 0, "f").write(table.io(), root);
    poller.pollOnce();
    assertEquals(2, metrics.watchReportsSeen.sum());
    assertEquals(0, metrics.watchReportsMissed.sum(), "chained to the one seen");

    // a new file this time: (r0, g) is unclaimed, so the alert is delivered
    report("b3", "b2-not-seen-here", fieldId, 1, "g").write(table.io(), root);
    poller.pollOnce();
    assertEquals(3, metrics.watchReportsSeen.sum());
    assertEquals(1, metrics.watchReportsMissed.sum(), "its predecessor was not the last seen: a build went unreported");
    assertEquals(3, metrics.watchReportAlertsDelivered.sum());
    assertEquals(3, sink.count(), "counted delivered means handed to the sink, not merely counted");
  }

  /**
   * A rule spanning columns has no single column, so asking the schema for {@code null} failed
   * the whole table's poll every interval, taking even a single-column rule's report on that
   * table down with it. The poller reads the reports of every column any rule names. Verified red
   * against {@code map(WatchRule::column)}: an NPE inside the poll, and no report seen.
   */
  @Test
  void aRuleSpanningColumnsDoesNotTakeTheTablesPollDown() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha");
    FormatConfig format = Records.with(
        LocalTableFixture.config().format(), Map.of("indexRoot", "file:" + tmp.resolve("index")));
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    String root = IndexPaths.root(table, format.indexRoot());
    Metrics metrics = new Metrics();
    RecordingSink sink = new RecordingSink();
    WatchRules rules = WatchRules.fixed(List.of(crossColumn()), metrics);
    ReportPoller poller = new ReportPoller(rules, source(table), format, CONFIG,
        new Alerts(sink, metrics, "iceberg"), metrics);

    report("b1", null, fieldId, 1, "f").write(table.io(), root);
    poller.pollOnce();

    assertEquals(1, metrics.watchReportsSeen.sum(), "the report of a column the rule names is read");
    assertEquals(1, sink.count());
  }

  /**
   * A (rule, file) this process already alerted on — here as the row scan would have, first — is
   * not delivered again when a build report carries it; the other alert in the same report still
   * is.
   */
  // Verified red with the claim removed from the poller: both alerts delivered.
  @Test
  void aReportAlertOnAClaimedRuleAndFileIsNotDeliveredAgain() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha");
    FormatConfig format = Records.with(
        LocalTableFixture.config().format(), Map.of("indexRoot", "file:" + tmp.resolve("index")));
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    String root = IndexPaths.root(table, format.indexRoot());
    Metrics metrics = new Metrics();
    RecordingSink sink = new RecordingSink();
    Alerts alerts = new Alerts(sink, metrics, "iceberg");
    assertTrue(alerts.claim("r0", "f"), "the row scan got to (r0, f) first");
    ReportPoller poller = new ReportPoller(rules(metrics), source(table), format, CONFIG, alerts, metrics);

    report("b1", null, fieldId, 2, "f").write(table.io(), root); // (r0, f) claimed; (r1, f) not
    poller.pollOnce();

    assertEquals(1, metrics.watchReportsSeen.sum());
    assertEquals(1, metrics.watchReportAlertsDelivered.sum(), "only the unclaimed alert");
    assertEquals(1, sink.count());
    assertEquals("r1", MAPPER.readTree(sink.json(0)).path("rule").path("id").asText());
  }
}
