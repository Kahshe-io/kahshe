package io.kahshe.watch.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kahshe.common.Metrics;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.indexer.build.IndexBuilder;
import io.kahshe.watch.rules.WatchRule;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The result set is what an analyst keeps: one summary line, then one line per file that needs
 * their attention. It must carry the whole partition's counts, list exactly the hits and the
 * unresolved files, and say what was asked — a result file that cannot be tied back to its query
 * is a number without a question.
 */
class HuntResultTest {
  @TempDir Path tmp;

  private static final String COLUMN = LocalTableFixture.COLUMN;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static List<JsonNode> lines(HuntPass.Result p) throws Exception {
    StringBuilder sb = new StringBuilder();
    p.writeJsonl(sb);
    List<JsonNode> out = new ArrayList<>();
    for (String line : sb.toString().split("\n")) {
      if (!line.isBlank()) {
        out.add(MAPPER.readTree(line));
      }
    }
    return out;
  }

  /**
   * One line per hit and per unresolved file, none per miss, one summary whose counts match the
   * lines. Red by deleting the unresolved loop in {@code writeJsonl} — the file that most needs
   * the analyst's attention is then the one missing from the file — and separately by listing
   * misses too, which on 220 files is 219 lines burying one.
   */
  @Test
  void theResultFileListsHitsAndUnresolvedAndCountsMisses() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha needle");
    LocalTableFixture.appendFile(table, "f2.parquet", "bravo");
    table.refresh();
    IndexBuilder.buildColumn(table, COLUMN, config);
    String later = LocalTableFixture.appendFile(table, "f3.parquet", "charlie needle");
    table.refresh();
    HuntPass.Result p =
        new HuntPass(new TermIndex(config.format(), new Metrics())).hunt(table, COLUMN, "needle");
    assertEquals(1, p.hit().size());
    assertEquals(1, p.miss().size());
    assertEquals(List.of(later), p.unresolved());

    List<JsonNode> lines = lines(p);
    assertEquals(3, lines.size(), "summary + one hit + one unresolved; the miss is not listed");
    JsonNode summary = lines.get(0);
    assertEquals("summary", summary.get("kind").asText());
    assertEquals(1, summary.get("files").get("hit").asInt());
    assertEquals(1, summary.get("files").get("miss").asInt());
    assertEquals(1, summary.get("files").get("unresolved").asInt());
    assertEquals("partial", summary.get("files").get("coverage").asText());
    assertNull(summary.get("hit"), "the summary carries counts, not the path arrays");

    List<String> verdicts = new ArrayList<>();
    for (JsonNode line : lines.subList(1, lines.size())) {
      assertEquals("file", line.get("kind").asText());
      verdicts.add(line.get("verdict").asText() + " " + line.get("path").asText());
    }
    assertEquals(List.of("hit " + p.hit().get(0), "unresolved " + later), verdicts);
    assertFalse(verdicts.stream().anyMatch(v -> v.startsWith("miss")),
        "a miss needs no action and is counted, not listed");
  }

  /**
   * The summary says what was asked and of what: the table, the column, the tokens actually
   * probed under the index's analyzer, and the rule id when there was one. Red by deleting any of
   * those puts in {@code toJson}.
   */
  @Test
  void theSummaryNamesTheQueryAndTheTable() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "Alpha bravo");
    IndexBuilder.buildColumn(table, COLUMN, config);
    HuntPass hunt = new HuntPass(new TermIndex(config.format(), new Metrics()));

    JsonNode term = lines(hunt.hunt(table, COLUMN, "Alpha")).get(0);
    assertEquals(table.name(), term.get("table").asText());
    assertEquals(COLUMN, term.get("column").asText());
    assertEquals("alpha", term.get("terms").get(0).asText(),
        "the token as the index's analyzer wrote it, not the value as typed");
    assertNull(term.get("rule"), "no rule was hunted");

    WatchRule rule = WatchRule.singleColumn("ioc-1", "t", WatchRule.Severity.HIGH, "p", "logs.t",
        COLUMN, List.of("alpha", "bravo"), List.of(), WatchRule.Condition.ANY_OF, 1);
    JsonNode byRule = lines(hunt.hunt(table, rule)).get(0);
    assertEquals("ioc-1", byRule.get("rule").asText());
    assertEquals(2, byRule.get("terms").size());
    assertTrue(byRule.get("files").get("hit").asInt() == 1);
  }

  /**
   * The confirmation SQL is over the files a matching row could be in — the hits and the
   * unresolved — and never a miss, which the dictionary proved cannot match. It rides in the
   * summary, on stdout and in the file alike, and only once asked for: a partition nobody asked
   * to confirm carries no query. Red by building it over {@code hit} alone (the unresolved file —
   * the one the engine must read to close the gap — drops out), and separately by including
   * {@code miss} (a query over a file the hunt proved clean).
   */
  @Test
  void theConfirmationSqlCoversHitsAndUnresolvedAndNeverAMiss() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha needle");
    String missed = LocalTableFixture.appendFile(table, "f2.parquet", "bravo");
    table.refresh();
    IndexBuilder.buildColumn(table, COLUMN, config);
    String later = LocalTableFixture.appendFile(table, "f3.parquet", "charlie needle");
    table.refresh();
    HuntPass hunt = new HuntPass(new TermIndex(config.format(), new Metrics()));

    HuntPass.Result bare = hunt.hunt(table, COLUMN, "needle");
    assertNull(bare.confirmationSql(), "not asked for, not built");
    assertNull(bare.toJson().get("confirmation_sql"));

    HuntPass.Result p = bare.withConfirmationSql("iceberg", "logs", "t");
    String sql = p.confirmationSql();
    assertTrue(sql.contains("FOR VERSION AS OF " + p.snapshotId()), sql);
    assertTrue(sql.contains(p.hit().get(0)), "the hit is pinned: " + sql);
    assertTrue(sql.contains(later), "the unresolved file is pinned — that is the scan the hunt "
        + "declined, handed to the engine: " + sql);
    assertFalse(sql.contains(missed), "a miss is never pinned; it cannot match: " + sql);
    assertTrue(sql.endsWith("AND (position('needle' IN lower(\"msg\")) > 0)"), sql);
    assertEquals(sql, p.toJson().get("confirmation_sql").asText());
    assertEquals(sql, lines(p).get(0).get("confirmation_sql").asText());

    // Nothing to confirm: no hit and no unresolved file leaves the SQL null, not a table scan.
    HuntPass.Result none = hunt.hunt(table, COLUMN, "zzzabsentzzz");
    assertEquals(1, none.unresolved().size(), "precondition: the uncovered file is unresolved");
    Table fullyCovered = LocalTableFixture.createTable(tmp.resolve("two"), "alpha");
    IndexBuilder.buildColumn(fullyCovered, COLUMN, config);
    HuntPass.Result covered = new HuntPass(new TermIndex(config.format(), new Metrics()))
        .hunt(fullyCovered, COLUMN, "zzz");
    assertTrue(covered.hit().isEmpty() && covered.unresolved().isEmpty(),
        "precondition: every file covered, none holds the term");
    assertNull(covered.withConfirmationSql("iceberg", "logs", "t").confirmationSql(),
        "no candidate file, no query");
  }
}
