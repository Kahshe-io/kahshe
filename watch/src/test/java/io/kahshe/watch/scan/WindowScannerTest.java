package io.kahshe.watch.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kahshe.common.Metrics;
import io.kahshe.format.FormatConfig;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.watch.Alerts;
import io.kahshe.watch.RecordingSink;
import io.kahshe.watch.rules.WatchRules;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Window rules: N matching rows for one key within T, counted ACROSS files.
 *
 * <p>The fixture's {@code num} column stands in for event time — {@link WindowScanner} reads an
 * INTEGER column as epoch milliseconds — and {@code other} for the key, so a rule reads "three
 * failures for one account inside the window".
 *
 * <p>The load-bearing test is {@link #countsAcrossFilesWhereAPerFileThresholdCannot}: it appends
 * the events in two commits and scans twice with ONE pass, which is the shape a per-file count
 * gets wrong. It asserts both halves — the window fires, and the same data under a per-file
 * {@code min_count} does not — because the second half is the whole reason this scanner exists.
 * Verified red with the counters' cross-file state removed (a fresh WindowCounters per file):
 * the window stops firing.
 */
class WindowScannerTest {
  private static final String PREFIX = "lakehouse";
  private static final TableIdentifier IDENT = TableIdentifier.parse("logs.events");
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @TempDir Path dir;
  private int table;
  private Metrics metrics;

  @BeforeEach
  void quietTheTestScanner() {
    CountingScanner.reset();
    CountingScanner.enabled = false;
    metrics = new Metrics();
  }

  /** Three failures for one key inside a wide window, but split across two commits. */
  @Test
  void countsAcrossFilesWhereAPerFileThresholdCannot() throws Exception {
    Table table = wide(new Object[] {"login failed", 1_000, "alice"});
    RecordingSink sink = new RecordingSink();
    ScanPass pass = pass(sink, windowRule("10s", 3));

    pass.scan(PREFIX, IDENT, table);
    assertEquals(0, sink.count(), "one event is not three");

    LocalTableFixture.appendRows(table, "f2.parquet",
        new Object[] {"login failed", 2_000, "alice"},
        new Object[] {"login failed", 3_000, "alice"});
    table.refresh();
    pass.scan(PREFIX, IDENT, table);

    assertEquals(1, sink.count(), "the third event completes a window opened in another file");
    assertEquals(1, metrics.watchWindowTrips.sum());

    // and the same three rows under a per-file threshold: the second file holds only two
    RecordingSink perFile = new RecordingSink();
    ScanPass flat = pass(perFile, """
        rules:
          - id: per-file
            severity: high
            prefix: lakehouse
            table: logs.events
            where: [{ column: msg, contains: [login failed] }]
            min_count: 3
        """);
    flat.scan(PREFIX, IDENT, table);
    assertEquals(0, perFile.count(),
        "a per-file count of the same data does not reach three: this is what the window is for");
  }

  @Test
  void eventsSpreadWiderThanTheTimeframeDoNotFire() throws Exception {
    Table table = wide(
        new Object[] {"login failed", 1_000, "alice"},
        new Object[] {"login failed", 5_000, "alice"},
        new Object[] {"login failed", 20_000, "alice"});
    RecordingSink sink = new RecordingSink();
    pass(sink, windowRule("10s", 3)).scan(PREFIX, IDENT, table);
    assertEquals(0, sink.count(), "19 s apart is not three within ten");
  }

  @Test
  void theCountIsPerKeyNotPerRule() throws Exception {
    Table table = wide(
        new Object[] {"login failed", 1_000, "alice"},
        new Object[] {"login failed", 1_100, "bob"},
        new Object[] {"login failed", 1_200, "alice"},
        new Object[] {"login failed", 1_300, "bob"},
        new Object[] {"login failed", 1_400, "alice"});
    RecordingSink sink = new RecordingSink();
    pass(sink, windowRule("10s", 3)).scan(PREFIX, IDENT, table);

    assertEquals(1, sink.count(), "alice reached three; bob has two");
    JsonNode alert = MAPPER.readTree(sink.json(0));
    assertEquals("alice", alert.path("window_key").asText());
    assertEquals("window", alert.path("evidence").path(0).path("kind").asText());
    assertEquals("exact", alert.path("evidence").path(0).path("confidence").asText());
    assertEquals(1_000, alert.path("window_start_ms").asLong());
    assertEquals(1_400, alert.path("window_end_ms").asLong());
  }

  /**
   * Two keys tripping in the same file must both alert. The pass claims one alert per (rule,
   * file); a window rule tells it what distinguishes them, or the second is suppressed as a
   * duplicate — which is a miss.
   */
  @Test
  void twoKeysTrippingInOneFileBothAlert() throws Exception {
    Table table = wide(
        new Object[] {"login failed", 1_000, "alice"},
        new Object[] {"login failed", 1_100, "bob"},
        new Object[] {"login failed", 1_200, "alice"},
        new Object[] {"login failed", 1_300, "bob"});
    RecordingSink sink = new RecordingSink();
    pass(sink, windowRule("10s", 2)).scan(PREFIX, IDENT, table);

    assertEquals(2, sink.count(), "one file, two keys, two alerts");
    assertEquals(2, metrics.watchWindowTrips.sum());
  }

  /** A threshold fires once per N events, not once per event after the Nth. */
  @Test
  void firesOncePerNEventsRatherThanOnEveryEventAfterTheThreshold() throws Exception {
    Table table = wide(
        new Object[] {"login failed", 1_000, "alice"},
        new Object[] {"login failed", 1_100, "alice"},
        new Object[] {"login failed", 1_200, "alice"},
        new Object[] {"login failed", 1_300, "alice"});
    RecordingSink sink = new RecordingSink();
    pass(sink, windowRule("10s", 2)).scan(PREFIX, IDENT, table);
    assertEquals(2, sink.count(), "four events at a threshold of two is two trips, not three");
  }

  @Test
  void theConfirmationSqlAsksTheEngineForTheGroupedCount() throws Exception {
    Table table = wide(
        new Object[] {"login failed", 1_000, "alice"},
        new Object[] {"login failed", 1_200, "alice"});
    RecordingSink sink = new RecordingSink();
    pass(sink, windowRule("10s", 2)).scan(PREFIX, IDENT, table);

    String sql = MAPPER.readTree(sink.json(0)).path("confirmation_sql").asText();
    assertTrue(sql.startsWith("SELECT \"other\", count(*) AS n FROM iceberg.\"logs\".\"events\""), sql);
    // num is an INTEGRAL column of epoch millis, so the bound is the millis themselves;
    // from_unixtime would compare a bigint to a timestamp and could not run
    assertTrue(sql.contains("\"num\" BETWEEN 1000 AND 1200"), sql);
    assertTrue(sql.contains("GROUP BY \"other\""), sql);
    assertTrue(sql.contains("HAVING count(*) >= 2"), sql);
    assertTrue(sql.contains("position('login failed' IN lower(\"msg\"))"), sql);
    // the discriminator is the pass's business, never a sink's
    assertTrue(MAPPER.readTree(sink.json(0)).path(ScanPass.CLAIM_KEY).isMissingNode(),
        "the claim key must be stripped before delivery: " + sink.json(0));
  }

  /** A window with no group_by counts every matching row together. */
  @Test
  void anUngroupedWindowCountsEveryMatchingRow() throws Exception {
    Table table = wide(
        new Object[] {"login failed", 1_000, "alice"},
        new Object[] {"login failed", 1_100, "bob"},
        new Object[] {"login failed", 1_200, "carol"});
    RecordingSink sink = new RecordingSink();
    pass(sink, """
        rules:
          - id: any-three
            severity: high
            prefix: lakehouse
            table: logs.events
            where: [{ column: msg, contains: [login failed] }]
            window: { ts_column: num, timeframe: 10s, count: 3 }
        """).scan(PREFIX, IDENT, table);
    assertEquals(1, sink.count(), "three failures from anyone, inside the window");
  }

  // ---------------------------------------------------------------- durability by replay

  /**
   * The restart case, which is the whole reason a stateful scanner needed a durability story: a
   * window half-counted when the process died must not be silently lost. There is no checkpoint
   * — the DATA is the durable state, and a fresh pass replays the files inside the rule's own
   * timeframe to rebuild exactly what was in memory.
   *
   * <p>Verified red with {@code WindowScanner.replayMs} returning 0: the second process starts
   * empty, sees one event, and the window never fires.
   */
  @Test
  void aFreshPassReplaysTheFilesItNeedsAndTheWindowSurvivesTheRestart() throws Exception {
    Table table = wide(new Object[] {"login failed", 1_000, "alice"});
    RecordingSink first = new RecordingSink();
    pass(first, windowRule("10s", 3)).scan(PREFIX, IDENT, table);
    LocalTableFixture.appendRows(table, "f2.parquet",
        new Object[] {"login failed", 2_000, "alice"});
    table.refresh();
    assertEquals(0, first.count(), "two of three: the process dies here");

    // a new pass is a new process: no counters, no claims, nothing but the table
    Metrics restarted = new Metrics();
    metrics = restarted;
    RecordingSink after = new RecordingSink();
    ScanPass second = pass(after, windowRule("10s", 3));
    LocalTableFixture.appendRows(table, "f3.parquet",
        new Object[] {"login failed", 3_000, "alice"});
    table.refresh();
    second.scan(PREFIX, IDENT, table);

    assertEquals(1, after.count(), "the two earlier events were rebuilt from their files");
    assertTrue(restarted.watchReplayFilesRead.sum() >= 2,
        "both older files were re-read: " + restarted.watchReplayFilesRead.sum());
  }

  /** A replayed window says so, so a receiver can drop what it already has. */
  @Test
  void anAlertRaisedFromAReplayedFileIsMarked() throws Exception {
    Table table = wide(
        new Object[] {"login failed", 1_000, "alice"},
        new Object[] {"login failed", 1_100, "alice"});
    RecordingSink first = new RecordingSink();
    pass(first, windowRule("10s", 2)).scan(PREFIX, IDENT, table);
    assertEquals(1, first.count(), "it fires the first time through");

    metrics = new Metrics();
    RecordingSink after = new RecordingSink();
    LocalTableFixture.appendRows(table, "f2.parquet", new Object[] {"quiet", 9_000, "zed"});
    table.refresh();
    pass(after, windowRule("10s", 2)).scan(PREFIX, IDENT, table);

    assertEquals(1, after.count(), "a restart re-raises it rather than losing it");
    assertTrue(MAPPER.readTree(after.json(0)).path("replayed").asBoolean(),
        "and says it is a repeat: " + after.json(0));
  }

  /** Replay is for the scanners that hold state; the prospective ones must not see old files. */
  @Test
  void aRuleWithNoWindowIsNotReplayed() throws Exception {
    Table table = wide(new Object[] {"login failed", 1_000, "alice"});
    LocalTableFixture.appendRows(table, "f2.parquet", new Object[] {"quiet", 2_000, "zed"});
    table.refresh();
    RecordingSink sink = new RecordingSink();
    pass(sink, """
        rules:
          - id: plain
            severity: high
            prefix: lakehouse
            table: logs.events
            where: [{ column: msg, contains: [login failed] }]
        """).scan(PREFIX, IDENT, table);

    assertEquals(0, sink.count(),
        "the matching row is in the older file, which a prospective rule never reads");
    assertEquals(0, metrics.watchReplayFilesRead.sum(), "and nothing was replayed for it");
  }

  /**
   * Replay is bounded: a long timeframe on a busy table would otherwise read the world before the
   * watcher is watching. Truncating leaves state unrebuilt, which is a miss, so it is counted and
   * logged with the three ways out rather than absorbed. Verified red with the budget check
   * removed: nothing counts and every file is read.
   */
  @Test
  void aReplayThatExceedsItsBudgetIsTruncatedLoudly() throws Exception {
    Table table = wide(new Object[] {"login failed", 1_000, "alice"});
    for (int i = 2; i <= 5; i++) {
      LocalTableFixture.appendRows(table, "f" + i + ".parquet",
          new Object[] {"login failed", 1_000 + i * 100, "alice"});
    }
    table.refresh();

    RecordingSink sink = new RecordingSink();
    Path file = Files.createTempFile(dir, "rules", ".yaml");
    Files.writeString(file, windowRule("10s", 5));
    WatchRules rules = new WatchRules(file.toString(), metrics);
    Alerts alerts = new Alerts(sink, metrics, "iceberg");
    // a budget of one: four older files exist, three of them will not be replayed
    new ScanPass(new ScanContext(rules, alerts, metrics, 200_000, 1),
        LocalTableFixture.config().format(), 2).scan(PREFIX, IDENT, table);

    assertEquals(1, metrics.watchReplayTruncated.sum(), "the budget bit, and said so");
    assertEquals(1, metrics.watchReplayFilesRead.sum(), "it spent exactly its budget");
    assertEquals(0, sink.count(), "and the window it could not rebuild does not fire");
  }

  // ---------------------------------------------------------------- a real TIMESTAMP column

  /**
   * Every other rule column in these tests is a string or a number, so this is the only one that
   * reads a real {@code timestamp} — and a timestamp is the case that breaks. Iceberg's generic
   * data model hands one back as a {@code LocalDateTime} while {@code Schema.accessorForField} is
   * built for the INTERNAL representation, where it is a {@code Long} of microseconds, so the
   * accessor's checked cast throws on every row of every file and a rule naming a time column
   * cannot fire at all.
   *
   * <p>Verified red against the accessor: {@code IllegalStateException: Not an instance of
   * java.lang.Long}.
   */
  @Test
  void aWindowOnARealTimestampColumnFires() throws Exception {
    Path root = Files.createDirectories(dir.resolve("timed" + table++));
    Table timed = LocalTableFixture.createTimedTable(root);
    LocalTableFixture.appendTimedRows(timed, "f1.parquet",
        new Object[] {"login failed", 1, LocalDateTime.of(1998, 6, 12, 11, 7, 55)},
        new Object[] {"login failed", 1, LocalDateTime.of(1998, 6, 12, 11, 8, 30)},
        new Object[] {"login failed", 1, LocalDateTime.of(1998, 6, 12, 11, 9, 10)},
        new Object[] {"quiet", 1, LocalDateTime.of(1998, 6, 12, 11, 9, 20)});

    RecordingSink sink = new RecordingSink();
    pass(sink, """
        rules:
          - id: timestamped
            severity: high
            prefix: lakehouse
            table: logs.events
            where: [{ column: msg, contains: [login failed] }]
            window:
              ts_column: ts
              timeframe: 5m
              count: 3
        """).scan(PREFIX, IDENT, timed);

    assertEquals(1, sink.count(), "three failures inside five minutes of event time");
    JsonNode alert = MAPPER.readTree(sink.json(0));
    long start = alert.path("window_start_ms").asLong();
    long end = alert.path("window_end_ms").asLong();
    assertEquals(LocalDateTime.of(1998, 6, 12, 11, 7, 55).toInstant(ZoneOffset.UTC).toEpochMilli(),
        start, "the window's start is the first event's OWN time, not the scan's");
    assertEquals(75_000, end - start, "and its span is the 75 s between the first and the third");
  }

  /** A row whose time column is null cannot be placed in a window, and does not fail the file. */
  @Test
  void aNullTimestampIsSkippedRatherThanFailingTheScan() throws Exception {
    Path root = Files.createDirectories(dir.resolve("timed" + table++));
    Table timed = LocalTableFixture.createTimedTable(root);
    LocalTableFixture.appendTimedRows(timed, "f1.parquet",
        new Object[] {"login failed", 1, null},
        new Object[] {"login failed", 1, LocalDateTime.of(1998, 6, 12, 11, 8, 30)},
        new Object[] {"login failed", 1, LocalDateTime.of(1998, 6, 12, 11, 9, 10)});

    RecordingSink sink = new RecordingSink();
    pass(sink, """
        rules:
          - id: null-ts
            severity: low
            prefix: lakehouse
            table: logs.events
            where: [{ column: msg, contains: [login failed] }]
            window: { ts_column: ts, timeframe: 5m, count: 3 }
        """).scan(PREFIX, IDENT, timed);

    assertEquals(0, sink.count(), "two placeable events is not three");
  }

  // ---------------------------------------------------------- ordered, sequential reads

  /**
   * A window rule counts ACROSS rows and the pass reads files concurrently, so event time
   * interleaves between threads and a rule that should have fired does not — a detection lost,
   * and lost silently.
   *
   * <p>The shape that loses it: a file of NEWER events is committed before a file of older ones,
   * which is what a manifest may hand back and what a thread pool produces anyway. Unordered, the
   * newer events fill the key's buffer, each older one displaces the previous older one, and the
   * three that are genuinely within the timeframe are never held together — no alert, and no late
   * drop either, so the counter does not even see this one. Ordered by the time column and read
   * sequentially, the older file goes first and the rule fires.
   *
   * <p>Verified red with {@code WindowScanner.orderRowsBy} returning null: zero alerts.
   */
  @Test
  void anOlderFileCommittedLastStillFires() throws Exception {
    Path root = Files.createDirectories(dir.resolve("timed" + table++));
    Table timed = LocalTableFixture.createTimedTable(root);
    // committed FIRST, and an hour NEWER: these fill the buffer before the real burst arrives
    LocalTableFixture.appendTimedRows(timed, "newer.parquet",
        new Object[] {"login failed", 1, LocalDateTime.of(1998, 6, 12, 12, 0, 0)},
        new Object[] {"login failed", 1, LocalDateTime.of(1998, 6, 12, 12, 0, 1)});
    // committed SECOND, and older: three inside a minute, which is what the rule is looking for
    LocalTableFixture.appendTimedRows(timed, "older.parquet",
        new Object[] {"login failed", 1, LocalDateTime.of(1998, 6, 12, 11, 0, 0)},
        new Object[] {"login failed", 1, LocalDateTime.of(1998, 6, 12, 11, 0, 1)},
        new Object[] {"login failed", 1, LocalDateTime.of(1998, 6, 12, 11, 0, 2)});
    timed.refresh();

    RecordingSink sink = new RecordingSink();
    Path file = Files.createTempFile(dir, "rules", ".yaml");
    Files.writeString(file, """
        rules:
          - id: ordered
            severity: high
            prefix: lakehouse
            table: logs.events
            where: [{ column: msg, contains: [login failed] }]
            window: { ts_column: ts, timeframe: 5m, count: 3 }
        """);
    WatchRules rules = new WatchRules(file.toString(), metrics);
    Alerts alerts = new Alerts(sink, metrics, "iceberg");
    // four threads: the default, and the shape that interleaves event time across files
    new ScanPass(new ScanContext(rules, alerts, metrics, 200_000, 2_000),
        LocalTableFixture.config().format(), 4).scan(PREFIX, IDENT, timed);

    assertEquals(1, sink.count(), "the three events inside a minute are a window whatever order "
        + "their files were committed in");
    JsonNode alert = MAPPER.readTree(sink.json(0));
    assertEquals(2_000, alert.path("window_end_ms").asLong() - alert.path("window_start_ms").asLong(),
        "and it is the OLDER burst that fired, spanning its two seconds");
  }

  /** A rule with no window leaves the scan parallel: the cost is paid only where it buys something. */
  @Test
  void aRuleWithNoWindowDoesNotMakeTheScanSequential() throws Exception {
    WindowScanner scanner = new WindowScanner();
    Path file = Files.createTempFile(dir, "rules", ".yaml");
    Files.writeString(file, """
        rules:
          - id: no-window
            severity: low
            prefix: lakehouse
            table: logs.events
            where: [{ column: msg, contains: [login failed] }]
        """);
    scanner.configure(new ScanContext(new WatchRules(file.toString(), metrics),
        new Alerts(new RecordingSink(), metrics, "iceberg"), metrics, 200_000, 2_000));
    assertNull(scanner.orderRowsBy(new TableView("logs.events",
            java.util.Map.of("msg", io.kahshe.analysis.ValueKind.STRING))),
        "nothing here counts across rows");
  }

  private static String windowRule(String timeframe, int count) {
    return """
        rules:
          - id: brute-force
            title: Repeated login failures for one account
            severity: high
            prefix: lakehouse
            table: logs.events
            where: [{ column: msg, contains: [login failed] }]
            window:
              ts_column: num
              timeframe: %s
              group_by: [other]
              count: %d
        """.formatted(timeframe, count);
  }

  private Table wide(Object[]... rows) throws IOException {
    Path root = Files.createDirectories(dir.resolve("t" + table++));
    Table created = LocalTableFixture.createWideTable(root);
    LocalTableFixture.appendRows(created, "f1.parquet", rows);
    return created;
  }

  private ScanPass pass(RecordingSink sink, String yaml) throws IOException {
    Path file = Files.createTempFile(dir, "rules", ".yaml");
    Files.writeString(file, yaml);
    WatchRules rules = new WatchRules(file.toString(), metrics);
    assertEquals(1, rules.current().size(), "the rules file must load, or the test proves nothing");
    Alerts alerts = new Alerts(sink, metrics, "iceberg");
    FormatConfig format = LocalTableFixture.config().format();
    return new ScanPass(new ScanContext(rules, alerts, metrics, 200_000), format, 2);
  }
}
