package io.kahshe.proxy.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.Metrics;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.proxy.TestConfigs;
import io.kahshe.proxy.catalog.BackendCatalogs;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.requests.PlanTableScanRequestParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Every served plan leaves one INFO line an operator can grep and a per-table count a dashboard
 * can plot: which table, which snapshot, who — the token's hash, never the token — how many files
 * went in and came out, and how long it took. No file path, ever. The log is read off stderr,
 * which is where slf4j-simple writes and what the test JVM has. Verified red with the line and
 * the counters absent: the pattern found nothing, and the table's adder was null.
 */
class PlanAuditTest {
  @TempDir Path tmp;

  private static final String TOKEN = "s3cret-token-value";

  private static String captureStderr(Runnable body) {
    PrintStream original = System.err;
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    PrintStream capture = new PrintStream(buffer, true, StandardCharsets.UTF_8);
    System.setErr(capture);
    try {
      body.run();
    } finally {
      System.setErr(original);
    }
    capture.flush();
    return buffer.toString(StandardCharsets.UTF_8);
  }

  private static String callerId() throws Exception {
    byte[] digest = MessageDigest.getInstance("SHA-256").digest(TOKEN.getBytes(StandardCharsets.UTF_8));
    return HexFormat.of().formatHex(digest).substring(0, 8);
  }

  @Test
  void aServedPlanWritesOneAuditLineAndCountsAgainstItsTable() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Metrics metrics = new Metrics();
    HadoopCatalog catalog = new HadoopCatalog(new Configuration(), tmp.toString());
    TableIdentifier ident = TableIdentifier.of("logs", "events");
    Table table = catalog.createTable(ident,
        new Schema(Types.NestedField.required(1, LocalTableFixture.COLUMN, Types.StringType.get())));
    LocalTableFixture.appendFile(table, "f1.parquet", "alpha");
    LocalTableFixture.appendFile(table, "f2.parquet", "bravo");
    long snapshot = table.currentSnapshot().snapshotId();
    PlanService service = new PlanService(
        metrics, new TermIndex(config.format(), metrics), TestConfigs.proxyConfig(), config.format());
    BackendCatalogs.PlanningCatalog planning = new BackendCatalogs.PlanningCatalog("service|test", catalog);
    // eq on a column with per-file bounds: the stats pass keeps one of the two files
    PlanTableScanRequest request = PlanTableScanRequestParser.fromJson(
        "{\"filter\":{\"type\":\"eq\",\"term\":\"" + LocalTableFixture.COLUMN + "\",\"value\":\"alpha\"}}");

    String log = captureStderr(
        () -> service.plan(planning, ident, request, List.of(), "Bearer " + TOKEN));

    Matcher line = Pattern.compile(
        "plan table=logs\\.events snapshot=" + snapshot + " caller=" + callerId()
            + " files_in=2 files_kept=1 ms=\\d+").matcher(log);
    assertTrue(line.find(), "no audit line in:\n" + log);
    assertFalse(log.contains(TOKEN), "the token itself never reaches the log:\n" + log);
    assertFalse(line.group().contains(".parquet"), "nor does a path: " + line.group());
    String key = Metrics.labels("table", "logs.events");
    assertEquals(1, metrics.tablePlanRequests.get(key).sum());
    assertEquals(1, metrics.tablePlanFilesKept.get(key).sum());

    String anonymous = captureStderr(() -> service.plan(planning, ident, request, List.of(), null));
    assertTrue(anonymous.contains(" caller=none "), "no bearer says so:\n" + anonymous);
    assertEquals(2, metrics.tablePlanRequests.get(key).sum());
  }
}
