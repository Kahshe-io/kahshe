package io.kahshe.watch.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The row scan: rules spanning columns, evaluated per ROW, on a table with no index anywhere.
 *
 * <p>Every table here is written by the fixture and never indexed — no index root, no
 * {@code kahshe.index} property, no build. That is the point of the pass: what it reads is
 * decided by the rules.
 */
class ScanPassTest {
  private static final String PREFIX = "lakehouse";
  private static final TableIdentifier IDENT = TableIdentifier.parse("logs.events");
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @TempDir Path dir;
  private int table;

  @BeforeEach
  void quietTheTestScanner() {
    CountingScanner.reset();
    CountingScanner.enabled = false;
  }

  @AfterEach
  void resetTheTestScanner() {
    CountingScanner.enabled = false;
  }

  /**
   * The case a cross-column rule exists for: each field is satisfied, by DIFFERENT rows. A
   * file-level conjunction says "this file holds a Service Control Manager row and a 7045 row"
   * and fires; the rule asked for a row that is both.
   */
  @Test
  void allOfAcrossColumnsDoesNotFireWhenTheFieldsMatchDifferentRows() throws Exception {
    Table table = wide(
        new Object[] {"Service Control Manager", 1, "ok"},
        new Object[] {"Print Spooler", 7045, "ok"});
    RecordingSink sink = scan(table, twoField());
    assertEquals(0, sink.count(),
        "two fields satisfied by two different rows is not a row that satisfies both");
  }

  @Test
  void allOfAcrossColumnsFiresOnTheRowThatSatisfiesBoth() throws Exception {
    Table table = wide(
        new Object[] {"Service Control Manager", 1, "ok"},
        new Object[] {"Service Control Manager", 7045, "ok"},
        new Object[] {"Print Spooler", 7045, "ok"});
    RecordingSink sink = scan(table, twoField());

    assertEquals(1, sink.count());
    JsonNode alert = MAPPER.readTree(sink.json(0));
    assertEquals("moriya", alert.path("rule").path("id").asText());
    assertEquals("critical", alert.path("rule").path("severity").asText());
    assertEquals("scan", alert.path("build_kind").asText());
    assertEquals(1, alert.path("matched_rows").asInt(), "one row satisfies both fields");
    assertEquals(1, alert.path("matched_row_positions").path(0).asInt(),
        "and it is the second row of the file");
    assertEquals(2, alert.path("evidence").size());
    for (JsonNode entry : alert.path("evidence")) {
      assertEquals("exact", entry.path("confidence").asText(),
          "the scan read the value; there is nothing advisory about it");
    }
  }

  /** {@code equals} on an int column compares numerically: 07045 in the rule is 7045 in the data. */
  @Test
  void numericEqualsOnAnIntColumn() throws Exception {
    Table table = wide(new Object[] {"anything", 7045, null});
    RecordingSink sink = scan(table, """
        rules:
          - id: numeric
            severity: high
            prefix: lakehouse
            table: logs.events
            where:
              - { column: num, equals: ["07045"] }
        """);

    assertEquals(1, sink.count());
    JsonNode alert = MAPPER.readTree(sink.json(0));
    assertEquals("equals", alert.path("evidence").path(0).path("kind").asText());
    assertEquals("num", alert.path("evidence").path(0).path("column").asText());
    assertTrue(alert.path("confirmation_sql").asText().contains("\"num\" = 07045"),
        "a numeric column's literal is not quoted: " + alert.path("confirmation_sql").asText());
  }

  /**
   * {@code equals_ignore_case} matches the row {@code equals} misses, and only that one.
   *
   * <p>This is the operator Sigma libraries need: Sigma's plain {@code field: value} is
   * case-insensitive in most implementations, so a converted rule matching a cased string has to
   * reach this operator, or it quietly matches fewer rows on kahshe than it does anywhere else.
   */
  @Test
  void equalsIgnoreCaseMatchesACasedValueAndEqualsDoesNot() throws Exception {
    Table cased = wide(new Object[] {"Service Control Manager", 1, null});

    assertEquals(0, scan(cased, rule("equals", "service control manager")).count(),
        "equals is exact and case-sensitive, and stays that way");
    assertEquals(1, scan(cased, rule("equals_ignore_case", "service control manager")).count());
    assertEquals(1, scan(cased, rule("equals_ignore_case", "SERVICE CONTROL MANAGER")).count(),
        "the fold is on both sides, not just the rule's");
  }

  /** Case-insensitive, not substring: the fold must not quietly widen the operator. */
  @Test
  void equalsIgnoreCaseIsStillTheWholeValue() throws Exception {
    Table cased = wide(new Object[] {"Service Control Manager", 1, null});
    assertEquals(0, scan(cased, rule("equals_ignore_case", "service control")).count(),
        "a prefix is not the whole value; that operator is starts_with");
    assertEquals(0, scan(cased, rule("equals_ignore_case", "control")).count());
  }

  /**
   * The numeric equivalence survives the fold: {@code "07045"} still meets 7045, because the
   * value is canonicalised BEFORE it is lowered rather than compared as the text it was typed as.
   */
  @Test
  void equalsIgnoreCaseKeepsTheCanonicalFormOnANumericColumn() throws Exception {
    Table table = wide(new Object[] {"anything", 7045, null});
    RecordingSink sink = scan(table, """
        rules:
          - id: folded
            severity: high
            prefix: lakehouse
            table: logs.events
            where:
              - { column: num, equals_ignore_case: ["07045"] }
        """);
    assertEquals(1, sink.count());
    JsonNode alert = MAPPER.readTree(sink.json(0));
    assertEquals("equals_ignore_case", alert.path("evidence").path(0).path("kind").asText());
    // And the SQL asks what the scan asked. On a number there is no case to fold, so folding is
    // dropped rather than applied to the text "07045", which would have matched nothing while
    // the scan matched — the scan-disagrees-with-its-own-SQL failure, caught by this assertion.
    assertTrue(alert.path("confirmation_sql").asText().contains("\"num\" = 07045"),
        alert.path("confirmation_sql").asText());
  }

  /**
   * A merge-on-read snapshot: the scan reads raw rows and applies no delete file, so its counts
   * are upper bounds and the evidence must say so.
   *
   * <p>An alert claiming {@code exact} on a delete-bearing table sends an analyst a confirmation
   * query that applies the deletes and returns FEWER rows — and a short confirmation does not read
   * as a caveat, it reads as a false positive. Verified red by restoring the literal "exact".
   */
  @Test
  void evidenceFromADeleteBearingSnapshotIsAdvisoryRatherThanExact() throws Exception {
    Table table = wide(new Object[] {"login failed", 1, "alice"});
    table.updateProperties().commit();
    RecordingSink sink = scan(table, """
        rules:
          - id: mor
            severity: high
            prefix: lakehouse
            table: logs.events
            where: [{ column: msg, contains: [login failed] }]
        """);

    assertEquals(1, sink.count());
    JsonNode alert = MAPPER.readTree(sink.json(0));
    assertEquals("exact", alert.path("evidence").path(0).path("confidence").asText(),
        "an ordinary copy-on-write append proves itself delete-free, so evidence stays exact");
    assertEquals("0", table.currentSnapshot().summary().get("total-delete-files"),
        "the fixture must be delete-free, or the assertion above proves nothing");
  }

  /**
   * The decision itself, on both branches — the integration test above can only reach the
   * delete-free one, because writing a real position-delete file is work this suite does not do.
   *
   * <p>An ABSENT summary key counts as delete-bearing. A summary that does not prove the absence
   * of deletes is not the same as one that proves it, and between "claim exact and be wrong" and
   * "claim advisory and be pessimistic" only the second is safe: a short confirmation reads as a
   * false positive, and that is the failure this exists to prevent.
   */
  @Test
  void aSnapshotThatDoesNotProveItIsDeleteFreeIsTreatedAsDeleteBearing() {
    assertTrue(ScanPass.deleteBearing(null), "no snapshot at all cannot prove anything");
    assertTrue(ScanPass.deleteBearing(summarised(java.util.Map.of())),
        "an absent total-delete-files is not a proof of zero");
    assertTrue(ScanPass.deleteBearing(summarised(java.util.Map.of("total-delete-files", "3"))));
    assertFalse(ScanPass.deleteBearing(summarised(java.util.Map.of("total-delete-files", "0"))),
        "only an explicit zero earns exact evidence");
  }

  /** And what each verdict is worth to a scanner. */
  @Test
  void theContextTurnsThatVerdictIntoTheConfidenceItReports() {
    assertEquals("exact", ctx(false).confidence());
    assertEquals("advisory", ctx(true).confidence(),
        "rows a delete file removed are still in the data file the scan reads, so a count over "
            + "them is an upper bound and must not be labelled exact");
  }

  private static FileScanContext ctx(boolean deleteBearing) {
    return new FileScanContext("lakehouse", "logs", "events", 1L, "f.parquet", false,
        deleteBearing, java.util.Map.of(), java.util.Set.of());
  }

  /** A snapshot that carries nothing but the summary the decision reads. */
  private static org.apache.iceberg.Snapshot summarised(java.util.Map<String, String> summary) {
    return (org.apache.iceberg.Snapshot) java.lang.reflect.Proxy.newProxyInstance(
        ScanPassTest.class.getClassLoader(),
        new Class<?>[] {org.apache.iceberg.Snapshot.class},
        (proxy, method, args) -> "summary".equals(method.getName()) ? summary
            : method.getReturnType().isPrimitive() ? 0L : null);
  }

  private static String rule(String op, String value) {
    return """
        rules:
          - id: folded
            severity: high
            prefix: lakehouse
            table: logs.events
            where:
              - { column: msg, %s: ["%s"] }
        """.formatted(op, value);
  }

  /** A different int value in the same column must not fire — the comparison is a value, not a substring. */
  @Test
  void numericEqualsDoesNotFireOnAnotherNumber() throws Exception {
    Table table = wide(new Object[] {"anything", 17045, null});
    assertEquals(0, scan(table, """
        rules:
          - id: numeric
            severity: high
            prefix: lakehouse
            table: logs.events
            where:
              - { column: num, equals: [7045] }
        """).count());
  }

  /**
   * A column no index covers, on a table with no index at all: coverage is the schema's, not
   * {@code kahshe.index}'s, so the rule fires anyway.
   */
  @Test
  void firesOnAColumnNoIndexCovers() throws Exception {
    Table table = wide(new Object[] {"nothing here", 1, "ZzNetSvc installed"});
    assertFalse(table.properties().containsKey("kahshe.index"),
        "the fixture must not declare an indexed column, or this proves nothing");
    RecordingSink sink = scan(table, """
        rules:
          - id: unindexed
            severity: medium
            prefix: lakehouse
            table: logs.events
            where:
              - { column: other, contains: [zznetsvc] }
        """);

    assertEquals(1, sink.count());
    assertEquals("other",
        MAPPER.readTree(sink.json(0)).path("evidence").path(0).path("column").asText());
  }

  /** The alert has to carry the whole condition, or an operator confirms a rule nobody wrote. */
  @Test
  void confirmationSqlCarriesEveryFieldsPredicate() throws Exception {
    Table table = wide(
        new Object[] {"Service Control Manager", 7045, "ZzNetSvc service installed"});
    RecordingSink sink = scan(table, moriya());
    String sql = MAPPER.readTree(sink.json(0)).path("confirmation_sql").asText();

    assertTrue(sql.startsWith("SELECT * FROM iceberg.\"logs\".\"events\" FOR VERSION AS OF "), sql);
    assertTrue(sql.contains("\"$path\" = '" + table.location() + "/data/f1.parquet'"), sql);
    assertTrue(sql.contains("\"msg\" = 'Service Control Manager'"), sql);
    assertTrue(sql.contains("\"num\" = 7045"), sql);
    assertTrue(sql.contains("position('zznetsvc' IN lower(\"other\")) > 0"), sql);
    assertFalse(sql.contains(" OR "), "an all-of rule's SQL never disjoins its fields: " + sql);
  }

  // ------------------------------------------------ the detection form and the new operators

  /** Sigma's own shape: a selection, a filter, and {@code selection and not filter}. */
  private static String sigmaWithFilter() {
    return """
        rules:
          - id: sigma-filter
            title: Service install that is not the benign one
            severity: high
            prefix: lakehouse
            table: logs.events
            detection:
              selection:
                - { column: msg, equals: ["Service Control Manager"] }
                - { column: num, equals: [7045] }
              filter:
                - { column: other, contains: [benign] }
              condition: selection and not filter
        """;
  }

  @Test
  void notExcludesTheRowTheFilterMatches() throws Exception {
    Table table = wide(new Object[] {"Service Control Manager", 7045, "benign updater"});
    assertEquals(0, scan(table, sigmaWithFilter()).count(),
        "the selection holds, but the filter excludes this row");
  }

  @Test
  void notKeepsTheRowTheFilterDoesNotMatch() throws Exception {
    Table table = wide(new Object[] {"Service Control Manager", 7045, "ZzNetSvc installed"});
    RecordingSink sink = scan(table, sigmaWithFilter());
    assertEquals(1, sink.count());
    assertEquals(1, MAPPER.readTree(sink.json(0)).path("matched_rows").asLong());
  }

  /**
   * A null column does not satisfy a field, so {@code not} of it holds — and the confirmation SQL
   * has to agree, which is why the negated term is wrapped in COALESCE: bare {@code NOT NULL} is
   * NULL in SQL and would return fewer rows than the alert counted.
   */
  @Test
  void notOnANullColumnFiresAndTheSqlAgrees() throws Exception {
    Table table = wide(new Object[] {"Service Control Manager", 7045, null});
    RecordingSink sink = scan(table, sigmaWithFilter());

    assertEquals(1, sink.count(), "a null 'other' cannot contain 'benign', so 'not filter' holds");
    String sql = MAPPER.readTree(sink.json(0)).path("confirmation_sql").asText();
    assertTrue(sql.contains("NOT COALESCE(position('benign' IN lower(\"other\")) > 0, false)"), sql);
  }

  @Test
  void oneOfThemFiresOnTheRowSatisfyingEitherSelection() throws Exception {
    String yaml = """
        rules:
          - id: either
            severity: low
            prefix: lakehouse
            table: logs.events
            detection:
              sel_a:
                - { column: msg, equals: ["alpha"] }
              sel_b:
                - { column: other, equals: ["bravo"] }
              condition: 1 of them
        """;
    assertEquals(1, scan(wide(new Object[] {"alpha", 1, "zulu"}), yaml).count(),
        "the first selection alone is enough");
    assertEquals(1, scan(wide(new Object[] {"zulu", 1, "bravo"}), yaml).count(),
        "so is the second");
    assertEquals(0, scan(wide(new Object[] {"zulu", 1, "zulu"}), yaml).count(),
        "and neither is not");
  }

  @Test
  void aRegexMatchesAndItsSqlAsksForARegex() throws Exception {
    Table table = wide(
        new Object[] {"svc-4711 started", 1, null},
        new Object[] {"svc-xyz started", 2, null});
    RecordingSink sink = scan(table, """
        rules:
          - id: re-rule
            severity: medium
            prefix: lakehouse
            table: logs.events
            where:
              - { column: msg, re: ["svc-[0-9]+"] }
        """);

    assertEquals(1, sink.count());
    JsonNode alert = MAPPER.readTree(sink.json(0));
    assertEquals(1, alert.path("matched_rows").asLong(), "only the numeric one");
    assertEquals("re", alert.path("evidence").path(0).path("kind").asText());
    assertTrue(alert.path("confirmation_sql").asText()
        .contains("regexp_like(\"msg\", 'svc-[0-9]+')"), alert.path("confirmation_sql").asText());
  }

  @Test
  void aNumericRangeOnAnIntColumnComparesAsNumbers() throws Exception {
    Table table = wide(
        new Object[] {"a", 404, null},
        new Object[] {"b", 500, null},
        new Object[] {"c", 200, null});
    RecordingSink sink = scan(table, """
        rules:
          - id: range
            severity: low
            prefix: lakehouse
            table: logs.events
            where:
              - { column: num, gte: [400], lt: [500] }
            condition: all-of
        """);

    assertEquals(1, sink.count());
    JsonNode alert = MAPPER.readTree(sink.json(0));
    assertEquals(1, alert.path("matched_rows").asLong(), "404 only: 500 is not < 500, 200 not >= 400");
    String sql = alert.path("confirmation_sql").asText();
    assertTrue(sql.contains("\"num\" >= 400"), sql);
    assertTrue(sql.contains("\"num\" < 500"), sql);
    assertFalse(sql.contains("TRY_CAST"), "a numeric column needs no cast: " + sql);
  }

  /** A range on a text column: the rows that are not numbers simply do not satisfy it. */
  @Test
  void aNumericRangeOnATextColumnSkipsNonNumbersAndCastsInSql() throws Exception {
    Table table = wide(
        new Object[] {"512", 1, null},
        new Object[] {"not a number", 2, null});
    RecordingSink sink = scan(table, """
        rules:
          - id: text-range
            severity: low
            prefix: lakehouse
            table: logs.events
            where:
              - { column: msg, gt: [500] }
        """);

    assertEquals(1, sink.count());
    JsonNode alert = MAPPER.readTree(sink.json(0));
    assertEquals(1, alert.path("matched_rows").asLong(), "the text row is not a number, not an error");
    assertTrue(alert.path("confirmation_sql").asText()
        .contains("TRY_CAST(\"msg\" AS DOUBLE) > 500"), alert.path("confirmation_sql").asText());
  }

  /** Once per rule per file per process, across passes: the claim is shared, not per-pass. */
  @Test
  void suppressesTheSecondAlertOnTheSameFile() throws Exception {
    Table table = wide(new Object[] {"Service Control Manager", 7045, "ZzNetSvc installed"});
    RecordingSink sink = new RecordingSink();
    Alerts alerts = new Alerts(sink, new Metrics(), "iceberg");
    WatchRules rules = rules(moriya());

    pass(alerts, rules).scan(PREFIX, IDENT, table);
    assertEquals(1, sink.count());
    // a second pass has its own last-scanned state, so it reads the same file again
    pass(alerts, rules).scan(PREFIX, IDENT, table);
    assertEquals(1, sink.count(), "the (rule, file) claim is shared, so the file alerts once");
  }

  /** The old single-column form, unchanged in the YAML, evaluated by the scanner. */
  @Test
  void theSingleColumnFormStillFires() throws Exception {
    Table table = wide(
        new Object[] {"connection error on shard 3", 1, null},
        new Object[] {"all good", 2, null});
    RecordingSink sink = scan(table, """
        rules:
          - id: error-burst
            title: Error burst in event logs
            severity: high
            prefix: lakehouse
            table: logs.events
            column: msg
            match: [error]
        """);

    assertEquals(1, sink.count());
    JsonNode alert = MAPPER.readTree(sink.json(0));
    assertEquals("match", alert.path("evidence").path(0).path("kind").asText());
    assertEquals("error", alert.path("evidence").path(0).path("values").path(0).asText());
    assertEquals(1, alert.path("matched_rows").asInt());
    assertTrue(alert.path("confirmation_sql").asText()
        .contains("position('error' IN lower(\"msg\")) > 0"));
  }

  /**
   * The seam: a scanner registered only in the test classpath's services file is discovered,
   * its column joins the projection, its row hook sees every row, and its alerts are delivered.
   *
   * <p>Verified by breaking it: with the services file removed the pass finds one scanner, the
   * counting scanner never runs, and its column is never read.
   */
  @Test
  void aScannerDiscoveredThroughTheClasspathIsRunAndDelivered() throws Exception {
    CountingScanner.enabled = true;
    Table table = wide(
        new Object[] {"Service Control Manager", 7045, "ZzNetSvc installed"},
        new Object[] {"quiet", 1, "nothing"});
    RecordingSink sink = new RecordingSink();
    Alerts alerts = new Alerts(sink, new Metrics(), "iceberg");
    ScanPass pass = pass(alerts, rules(moriya()));

    List<String> names = pass.scanners().stream().map(Scanner::name).toList();
    assertEquals(List.of(RuleScanner.NAME, WindowScanner.NAME, CountingScanner.NAME), names,
        "every scanner is discovered, the built-ins first and in their declared order");

    pass.scan(PREFIX, IDENT, table);

    assertEquals(List.of("ZzNetSvc installed", "nothing"), CountingScanner.SEEN,
        "the test scanner's column was projected and its row hook saw every row in order");
    assertEquals(2, sink.count(), "the rule's alert and the test scanner's alert both delivered");
    boolean sawTestAlert = false;
    for (int i = 0; i < sink.count(); i++) {
      sawTestAlert |= MAPPER.readTree(sink.json(i)).path("scanner").asText()
          .equals(CountingScanner.NAME);
    }
    assertTrue(sawTestAlert, "the discovered scanner's own payload reached the sink");
  }

  /** A scanner that wants nothing from this table is never opened, and costs no read. */
  @Test
  void aScannerAsksForNoColumnAndTheFileIsStillReadForTheOthers() throws Exception {
    CountingScanner.enabled = false; // columns() answers empty
    Table table = wide(new Object[] {"Service Control Manager", 7045, "ZzNetSvc installed"});
    assertEquals(1, scan(table, moriya()).count());
    assertTrue(CountingScanner.SEEN.isEmpty(), "a scanner asking for no column reads no row");
  }

  /** Two fields over two columns, so a file can satisfy each of them on a different row. */
  private static String twoField() {
    return """
        rules:
          - id: moriya
            title: ZzNetSvc service installation
            severity: critical
            prefix: lakehouse
            table: logs.events
            where:
              - { column: msg, equals: ["Service Control Manager"] }
              - { column: num, equals: [7045] }
            condition: all-of
            min_count: 1
        """;
  }

  private static String moriya() {
    return """
        rules:
          - id: moriya
            title: ZzNetSvc service installation
            severity: critical
            prefix: lakehouse
            table: logs.events
            where:
              - { column: msg, equals: ["Service Control Manager"] }
              - { column: num, equals: [7045] }
              - { column: other, contains: [ZzNetSvc] }
            condition: all-of
            min_count: 1
        """;
  }

  /** A fresh three-column table holding one data file of {@code {msg, num, other}} rows. */
  private Table wide(Object[]... rows) throws IOException {
    Path root = Files.createDirectories(dir.resolve("t" + table++));
    Table created = LocalTableFixture.createWideTable(root);
    LocalTableFixture.appendRows(created, "f1.parquet", rows);
    return created;
  }

  private RecordingSink scan(Table table, String yaml) throws IOException {
    RecordingSink sink = new RecordingSink();
    Alerts alerts = new Alerts(sink, new Metrics(), "iceberg");
    pass(alerts, rules(yaml)).scan(PREFIX, IDENT, table);
    return sink;
  }

  private WatchRules rules(String yaml) throws IOException {
    Path file = Files.createTempFile(dir, "rules", ".yaml");
    Files.writeString(file, yaml);
    WatchRules rules = new WatchRules(file.toString(), new Metrics());
    assertEquals(1, rules.current().size(), "the rules file must load, or the test proves nothing");
    return rules;
  }

  private static ScanPass pass(Alerts alerts, WatchRules rules) {
    FormatConfig format = LocalTableFixture.config().format();
    return new ScanPass(new ScanContext(rules, alerts), format, 2);
  }
}
