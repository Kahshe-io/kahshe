package io.kahshe.watch.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * A hunt's confirmation is the alert's confirmation with the pin inverted: several files instead
 * of one, and none when there are too many to name. What it must never do is ask a different
 * question from the one the partition answered, or ask it of a subset of the files and call that
 * complete.
 */
class HuntSqlTest {

  private static final List<WatchRule.Field> ONE =
      List.of(new WatchRule.Field("msg", WatchRule.Op.MATCH, List.of("needle")));
  private static final WatchRule.Expr FIRST = new WatchRule.Expr.FieldRef(0);

  /**
   * The shape: snapshot-scoped, the candidate files pinned as an {@code IN} list, the condition
   * parenthesised after it exactly as the per-file form parenthesises its own. Red by deleting
   * the {@code IN} clause.
   */
  @Test
  void pinsTheCandidateFilesByPathIn() {
    String sql = ConfirmationSql.hunt("iceberg", "logs", "events", 42,
        List.of("s3://b/f1.parquet", "s3://b/f3.parquet"), ONE, Set.of(), FIRST);
    assertEquals(
        "SELECT * FROM iceberg.\"logs\".\"events\" FOR VERSION AS OF 42"
            + " WHERE \"$path\" IN ('s3://b/f1.parquet', 's3://b/f3.parquet')"
            + " AND (position('needle' IN lower(\"msg\")) > 0)",
        sql);
  }

  /**
   * Over the cap the pin is DROPPED, not cut: a query pinned to the first N of N+1 candidate
   * files would return rows from a subset and read as complete. The unpinned form is slower and
   * whole. Red by replacing the cap branch with {@code paths.subList(0, PATH_PIN_MAX)}: the
   * query then names a path, and the one it leaves out is exactly the file it should have asked
   * about.
   */
  @Test
  void overTheCapDropsThePinRatherThanTruncatingIt() {
    List<String> paths = new ArrayList<>();
    for (int i = 0; i <= ConfirmationSql.PATH_PIN_MAX; i++) {
      paths.add("s3://b/f" + i + ".parquet");
    }
    String sql = ConfirmationSql.hunt("iceberg", "logs", "events", 42, paths, ONE, Set.of(), FIRST);
    assertFalse(sql.contains("$path"), "over the cap, no file is pinned: " + sql.length());
    assertFalse(sql.contains("s3://b/f0.parquet"), "and no subset of them either");
    assertTrue(sql.startsWith(
        "SELECT * FROM iceberg.\"logs\".\"events\" FOR VERSION AS OF 42 WHERE (position("));
  }

  /**
   * The oracle: the condition text is the alert's own, from the one tree compiler, including the
   * {@code NOT COALESCE(…, false)} a negated term needs so SQL's three-valued logic does not drop
   * a row the partition counted. A hunt over a {@code detection} rule with a {@code not} is
   * exactly where a second compiler would diverge. Red by replacing the {@code condition(...)}
   * call in {@code hunt} with a local join of {@code position()} terms.
   */
  @Test
  void theConditionIsTheAlertsCompiler() {
    List<WatchRule.Field> two = List.of(
        new WatchRule.Field("msg", WatchRule.Op.MATCH, List.of("alpha")),
        new WatchRule.Field("msg", WatchRule.Op.MATCH, List.of("bravo")));
    WatchRule.Expr aAndNotB = new WatchRule.Expr.And(List.of(
        new WatchRule.Expr.FieldRef(0), new WatchRule.Expr.Not(new WatchRule.Expr.FieldRef(1))));

    String alert = ConfirmationSql.build("iceberg", "logs", "events", 7, "s3://b/f1.parquet",
        two, Set.of(), aAndNotB);
    String hunt = ConfirmationSql.hunt("iceberg", "logs", "events", 7,
        List.of("s3://b/f1.parquet"), two, Set.of(), aAndNotB);

    String alertCondition = alert.substring(alert.indexOf(" AND (") + " AND ".length());
    String huntCondition = hunt.substring(hunt.indexOf(") AND (") + ") AND ".length());
    assertEquals(alertCondition, huntCondition,
        "the hunt must ask the rule as the alert asks it, negation and all");
    assertTrue(huntCondition.contains("NOT COALESCE("), huntCondition);
  }

  /**
   * No candidate file means no row can match — the dictionary proved it — and the honest SQL is
   * none at all. {@code IN ()} is not SQL, and an unpinned query over the whole table would be a
   * scan for something the hunt proved is not there. Red by deleting the emptiness check: the
   * result is {@code WHERE "$path" IN () AND …}.
   */
  @Test
  void noCandidateFileIsRefusedRatherThanEmittingInOfNothing() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> ConfirmationSql.hunt("iceberg", "logs", "events", 1, List.of(), ONE, Set.of(),
            FIRST));
    assertTrue(e.getMessage().contains("nothing to confirm"), e.getMessage());
  }
}
