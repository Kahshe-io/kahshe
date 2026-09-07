package io.kahshe.watch.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.analysis.ValueKind;
import io.kahshe.common.Metrics;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ConfirmationSqlTest {
  @Test
  void snapshotScopedShape() {
    String sql = ConfirmationSql.build("iceberg", "logs", "events", 42, "s3://b/f1.parquet",
        "msg", List.of("error"), false);
    assertEquals(
        "SELECT * FROM iceberg.\"logs\".\"events\" FOR VERSION AS OF 42"
            + " WHERE \"$path\" = 's3://b/f1.parquet'"
            + " AND (position('error' IN lower(\"msg\")) > 0)",
        sql);
  }

  @Test
  void singleQuotesInLiteralsEscaped() {
    String sql = ConfirmationSql.build("iceberg", "logs", "events", 1, "s3://b/o'brien.parquet",
        "msg", List.of("o'brien's key"), false);
    assertEquals(
        "SELECT * FROM iceberg.\"logs\".\"events\" FOR VERSION AS OF 1"
            + " WHERE \"$path\" = 's3://b/o''brien.parquet'"
            + " AND (position('o''brien''s key' IN lower(\"msg\")) > 0)",
        sql);
  }

  @Test
  void quotesAndDotsInIdentifiersEscaped() {
    String sql = ConfirmationSql.build("iceberg", "ns.dotted", "we\"ird", 1, "f",
        "co\"l.umn", List.of("abc", "def"), true);
    assertEquals(
        "SELECT * FROM iceberg.\"ns.dotted\".\"we\"\"ird\" FOR VERSION AS OF 1"
            + " WHERE \"$path\" = 'f'"
            + " AND (position('abc' IN lower(\"co\"\"l.umn\")) > 0"
            + " AND position('def' IN lower(\"co\"\"l.umn\")) > 0)",
        sql);
  }

  @Test
  void literalsAreLowercasedToMatchIndexSemantics() {
    String sql = ConfirmationSql.build("iceberg", "ns", "t", 1, "f", "c",
        List.of("BEGIN RSA"), false);
    assertEquals(
        "SELECT * FROM iceberg.\"ns\".\"t\" FOR VERSION AS OF 1 WHERE \"$path\" = 'f'"
            + " AND (position('begin rsa' IN lower(\"c\")) > 0)",
        sql);
  }

  // ------------------------------------------------------- the window bound and the reader's clock

  /**
   * The bound must mean the same instant on every engine session.
   *
   * <p>{@code from_unixtime} returns {@code timestamp(3) with time zone}; compared against a
   * zone-less {@code timestamp} column Trino coerces the column using the SESSION's zone, so the
   * alert's own query returns its rows under a UTC session and NOTHING under
   * {@code America/New_York} or {@code Asia/Tokyo}. An analyst pasting the alert's SQL into their
   * own client reads an empty result as a false positive, not as a broken query. A test has no
   * session zone of its own, so the literal spelling is the thing that has to be pinned.
   *
   * <p>Verified red: with the bound back to {@code from_unixtime}, this test fails.
   */
  @Test
  void aWindowBoundIsALiteralTheReadersSessionZoneCannotMove() throws Exception {
    String sql = windowSql(ValueKind.TIMESTAMP);
    assertTrue(sql.contains("\"ts\" BETWEEN TIMESTAMP '1998-06-12 11:11:53.000' "
        + "AND TIMESTAMP '1998-06-12 11:11:54.000'"), sql);
    assertFalse(sql.contains("from_unixtime"),
        "from_unixtime is timestamp WITH TIME ZONE and moves the window with the reader: " + sql);
  }

  /**
   * The same instant, written for a column that carries one. Against a {@code timestamptz} column
   * a zone-less literal is the mirror of the same bug — Trino would coerce the LITERAL in the
   * session's zone — so the two spellings are not interchangeable and the column's kind decides.
   */
  @Test
  void aZoneCarryingColumnGetsTheInstantMarkedUtc() throws Exception {
    assertTrue(windowSql(ValueKind.TIMESTAMPTZ)
        .contains("\"ts\" BETWEEN TIMESTAMP '1998-06-12 11:11:53.000 UTC' "
            + "AND TIMESTAMP '1998-06-12 11:11:54.000 UTC'"), windowSql(ValueKind.TIMESTAMPTZ));
  }

  /**
   * An integral column of epoch millis is one {@code WindowScanner.epochMillis} accepts and
   * windows correctly; comparing it against a timestamp instead is SQL that cannot run at all.
   * The column is left bare in every case so Iceberg's own min/max pruning still applies, which
   * is what keeps the confirmation query cheap on a large table.
   */
  @Test
  void anEpochMillisColumnIsComparedToMillis() throws Exception {
    assertTrue(windowSql(ValueKind.INTEGRAL).contains("\"ts\" BETWEEN 897649913000 AND 897649914000"),
        windowSql(ValueKind.INTEGRAL));
  }

  @Test
  void aDateColumnGetsADateLiteralAndAStringColumnIsoText() throws Exception {
    assertTrue(windowSql(ValueKind.DATE).contains("\"ts\" BETWEEN DATE '1998-06-12' AND DATE '1998-06-12'"),
        windowSql(ValueKind.DATE));
    assertTrue(windowSql(ValueKind.STRING)
        .contains("\"ts\" BETWEEN '1998-06-12T11:11:53Z' AND '1998-06-12T11:11:54Z'"),
        windowSql(ValueKind.STRING));
  }

  /** A window rule built through the loader, so the shape under test is the shipped one. */
  private static String windowSql(ValueKind tsKind) throws Exception {
    Path file = Files.createTempFile("rules", ".yaml");
    Files.writeString(file, """
        rules:
          - id: client-404s
            severity: medium
            prefix: lakehouse
            table: logs.httplogs
            where: [{ column: status, equals: [404] }]
            condition: all-of
            window: { ts_column: ts, timeframe: 1h, group_by: [clientip], count: 3 }
        """);
    WatchRule rule = new WatchRules(file.toString(), new Metrics()).current().get(0);
    return ConfirmationSql.window("iceberg", "logs", "httplogs", 483670635571079646L, rule,
        Set.of("status", "size"), tsKind, 897649913000L, 897649914000L);
  }
}
