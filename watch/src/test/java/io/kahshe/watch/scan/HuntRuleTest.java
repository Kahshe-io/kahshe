package io.kahshe.watch.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.Metrics;
import io.kahshe.format.IndexPruner;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.indexer.build.IndexBuilder;
import io.kahshe.watch.rules.WatchRule;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A rule hunts only in the shape whose file-level answer is its row-level answer, and every other
 * shape is refused by name — never half-answered, never quietly handed to a 48 GiB scan. The
 * shape is {@link WatchRule#ridesIndex}, read through {@link WatchRule#whyNotRidesIndex} so the
 * refusal and the boolean cannot disagree; two shapes a BUILD admits are refused on top.
 */
class HuntRuleTest {
  @TempDir Path tmp;

  private static final String COLUMN = LocalTableFixture.COLUMN;

  private static WatchRule single(List<String> match, List<String> contains,
      WatchRule.Condition condition, long minCount) {
    return WatchRule.singleColumn("r", "r", WatchRule.Severity.HIGH, "p", "logs.t", COLUMN,
        match, contains, condition, minCount);
  }

  private static WatchRule.Field field(String column, WatchRule.Op op, String value) {
    return new WatchRule.Field(column, op, List.of(value));
  }

  private static WatchRule flat(List<WatchRule.Field> where, WatchRule.Condition condition) {
    return new WatchRule("r", "r", WatchRule.Severity.HIGH, "p", "logs.t", where, condition, 1);
  }

  private static TermIndex reader(BuildConfig config) {
    return new TermIndex(config.format(), new Metrics());
  }

  private static List<String> prunerKeeps(Table table, BuildConfig config, TermIndex reader,
      List<String> tokens) throws Exception {
    Metrics metrics = new Metrics();
    IndexPruner pruner = new IndexPruner(reader, metrics, config.format());
    List<IndexPruner.ContainsHint> hints = new ArrayList<>();
    for (String token : tokens) {
      hints.add(new IndexPruner.ContainsHint(COLUMN, token, IndexPruner.HintKind.MATCH));
    }
    List<FileScanTask> kept =
        pruner.prune(table, null, hints, LocalTableFixture.planTasks(table));
    return kept.stream().map(t -> t.file().location()).sorted().toList();
  }

  /**
   * The quiet fallback: a rule the index cannot answer being answered anyway, half-evaluated per
   * file, or handed to a scan of every file the table holds. Each shape must be refused with the
   * reason that names it. Red by deleting the {@code whyNotRidesIndex} gate in
   * {@code hunt(Table, WatchRule)}: the multi-column and window rules then reach the partition
   * loop and either throw something that is not a refusal or return one.
   */
  @Test
  void everyShapeTheIndexCannotAnswerRefusesNamingIt() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha bravo");
    IndexBuilder.buildColumn(table, COLUMN, config);
    HuntPass hunt = new HuntPass(reader(config));

    List<WatchRule.Field> two = List.of(
        field(COLUMN, WatchRule.Op.MATCH, "alpha"), field(COLUMN, WatchRule.Op.MATCH, "bravo"));
    WatchRule windowed = new WatchRule("r", "r", WatchRule.Severity.HIGH, "p", "logs.t",
        List.of(field(COLUMN, WatchRule.Op.MATCH, "alpha")), WatchRule.Condition.ANY_OF, 1,
        WatchRule.Expr.flat(WatchRule.Condition.ANY_OF, 1),
        new WatchRule.Window("ts", 60_000, List.of(), 5));
    WatchRule negated = new WatchRule("r", "r", WatchRule.Severity.HIGH, "p", "logs.t",
        List.of(field(COLUMN, WatchRule.Op.MATCH, "alpha")), WatchRule.Condition.ANY_OF, 1,
        new WatchRule.Expr.Not(new WatchRule.Expr.FieldRef(0)));

    Map<String, WatchRule> shapes = Map.of(
        "window", windowed,
        "all-of", flat(two, WatchRule.Condition.ALL_OF),
        "operator re", flat(List.of(field(COLUMN, WatchRule.Op.RE, "a.*")),
            WatchRule.Condition.ANY_OF),
        "operator gt", flat(List.of(field(COLUMN, WatchRule.Op.GT, "5")),
            WatchRule.Condition.ANY_OF),
        "operator equals_ignore_case",
            flat(List.of(field(COLUMN, WatchRule.Op.EQUALS_IGNORE_CASE, "alpha")),
                WatchRule.Condition.ANY_OF),
        "not a flat any-of or all-of", negated,
        "spans columns", flat(List.of(field(COLUMN, WatchRule.Op.MATCH, "alpha"),
            field(LocalTableFixture.SECOND_COLUMN, WatchRule.Op.MATCH, "bravo")),
            WatchRule.Condition.ANY_OF));

    for (Map.Entry<String, WatchRule> shape : shapes.entrySet()) {
      HuntPass.Refused refused = assertThrows(HuntPass.Refused.class,
          () -> hunt.hunt(table, shape.getValue()),
          "the " + shape.getKey() + " shape was answered rather than refused");
      assertTrue(refused.getMessage().contains(shape.getKey()),
          "the refusal must name the shape '" + shape.getKey() + "': " + refused.getMessage());
    }
  }

  /**
   * The subtle one. The pruner DOES intersect two token bitmaps at file level, and a file holding
   * both tokens in different rows survives that intersection — sound for pruning, since a row
   * with both could only be in such a file, and unsound as a hunt result, since it is not
   * evidence that any row holds both. A hunt reporting that file as a hit would send an analyst
   * to a row that does not exist. Red by returning null from the all-of clause of
   * {@code whyNotRidesIndex}: the rule then rides, {@code Conditions.eval} ANDs two true
   * per-file verdicts, and the file is a hit.
   */
  @Test
  void anAllOfAcrossTwoFieldsRefusesEvenThoughThePrunerIntersects() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    // Two rows, one token each: the FILE holds both, no ROW does.
    Table table = LocalTableFixture.createTable(tmp, "alpha", "bravo");
    IndexBuilder.buildColumn(table, COLUMN, config);
    TermIndex reader = reader(config);

    List<String> kept = prunerKeeps(table, config, reader, List.of("alpha", "bravo"));
    assertEquals(1, kept.size(),
        "precondition: the pruner keeps the file, because a row with both could only be in it");

    HuntPass.Refused refused = assertThrows(HuntPass.Refused.class,
        () -> new HuntPass(reader).hunt(table,
            single(List.of("alpha", "bravo"), List.of(), WatchRule.Condition.ALL_OF, 1)));
    assertTrue(refused.getMessage().contains("all-of"), refused.getMessage());
    assertTrue(refused.getMessage().contains("not a row"), refused.getMessage());
  }

  /**
   * {@code contains} rides a build, where the file is read anyway and the gram set is free; it
   * does not ride a hunt. The gram tier's verdict is advisory — every gram present is not the
   * substring present — and on identifier-dense text every file holds every gram (measured 220 of
   * 220 on the lab corpus). A contains "hit" list would be the table. Red by deleting the
   * {@code contains} check: the literal is then probed as a token and the rule returns a
   * partition.
   */
  @Test
  void aContainsRuleRefusesNamingTheTier() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alphabet");
    IndexBuilder.buildColumn(table, COLUMN, config);

    HuntPass.Refused refused = assertThrows(HuntPass.Refused.class,
        () -> new HuntPass(reader(config)).hunt(table,
            single(List.of(), List.of("alpha"), WatchRule.Condition.ANY_OF, 0)));
    assertTrue(refused.getMessage().contains("contains"), refused.getMessage());
    assertTrue(refused.getMessage().contains("advisory"), refused.getMessage());
  }

  /**
   * {@code min_count} is a per-file occurrence threshold, read by the watcher off the per-file
   * term counts a build produces. The aggregate tier holds which files carry a term, not how many
   * times each; a hunt that ignored the threshold would fire a rule written to fire at fifty on a
   * file holding one. Red by deleting the {@code minCount} check.
   */
  @Test
  void aMinCountAboveOneRefuses() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha");
    IndexBuilder.buildColumn(table, COLUMN, config);

    HuntPass.Refused refused = assertThrows(HuntPass.Refused.class,
        () -> new HuntPass(reader(config)).hunt(table,
            single(List.of("alpha"), List.of(), WatchRule.Condition.ANY_OF, 50)));
    assertTrue(refused.getMessage().contains("min_count 50"), refused.getMessage());
  }

  /**
   * The one multi-field shape that rides: {@code any-of} over tokens on one column. Its hit set
   * is the UNION of the single-token hunts' — the oracle, two independent answers to compare
   * against — and an uncovered file is unresolved here exactly as it is for one token. Red by
   * replacing {@code Conditions.eval(expr, hits)} with {@code hits[0]}: the second token's file
   * becomes a miss.
   */
  @Test
  void anAnyOfOverTwoTokensIsTheUnionOfTheirFiles() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha");
    LocalTableFixture.appendFile(table, "f2.parquet", "bravo");
    LocalTableFixture.appendFile(table, "f3.parquet", "charlie");
    table.refresh();
    IndexBuilder.buildColumn(table, COLUMN, config);
    TermIndex reader = reader(config);
    HuntPass hunt = new HuntPass(reader);

    TreeSet<String> union = new TreeSet<>(hunt.hunt(table, COLUMN, "alpha").hit());
    union.addAll(hunt.hunt(table, COLUMN, "bravo").hit());
    assertEquals(2, union.size(), "precondition: the two tokens are in two different files");

    HuntPass.Result p = hunt.hunt(table,
        single(List.of("alpha", "bravo"), List.of(), WatchRule.Condition.ANY_OF, 1));
    assertEquals(List.copyOf(union), p.hit(), "any-of is the union of its tokens' files");
    assertEquals(1, p.miss().size(), "the file holding neither token is a miss");
    assertTrue(p.unresolved().isEmpty());

    // And the uncovered file lands where it does for one token: nowhere but unresolved.
    String later = LocalTableFixture.appendFile(table, "f4.parquet", "alpha");
    table.refresh();
    HuntPass.Result after = new HuntPass(reader(config)).hunt(table,
        single(List.of("alpha", "bravo"), List.of(), WatchRule.Condition.ANY_OF, 1));
    assertEquals(List.of(later), after.unresolved());
    assertEquals(p.hit(), after.hit(), "an uncovered file is not a hit however many tokens ask");
    assertFalse(after.miss().contains(later));
  }
}
