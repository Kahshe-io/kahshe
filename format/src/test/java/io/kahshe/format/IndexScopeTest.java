package io.kahshe.format;

import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.build.IndexBuilder;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.format.Coverage;
import io.kahshe.format.IndexPaths;
import io.kahshe.format.IndexPruner;
import io.kahshe.format.IndexScope;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.format.type.term.TermIndexWriter;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.expressions.Expressions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A table may declare which of its data files the index covers, and covering fewer must cost
 * pruning without ever costing correctness.
 *
 * <p>The second half is the one that matters and the one that is easy to get wrong. Most index
 * systems must be complete to be correct — a document missing from the index is missing from
 * results. kahshe's invariant is the opposite way round: a file the index does not know about is
 * KEPT, so a narrowed index returns the same rows as a complete one and merely scans more. These
 * tests assert that directly rather than trusting the invariant.
 */
class IndexScopeTest {
  @TempDir Path tmp;

  @Test
  void aScopedBuildCoversOnlyTheFilesTheExpressionAdmits() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha alpha");
    LocalTableFixture.appendFile(table, "f2.parquet", "zulu zulu");
    table.refresh();

    // only the file whose msg column can contain "alpha alpha"; Iceberg prunes the other by its
    // column statistics before a byte of it is read
    scope(table, Expressions.equal(LocalTableFixture.COLUMN, "alpha alpha"));

    BuildConfig config = LocalTableFixture.config();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    List<String> covered = coveredFiles(table, config);
    assertEquals(1, covered.size(), "the scope did not narrow coverage: " + covered);
    assertTrue(covered.get(0).endsWith("f1.parquet"), covered.get(0));
  }

  @Test
  void aFileOutsideTheScopeIsKeptRatherThanPruned() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha alpha");
    LocalTableFixture.appendFile(table, "f2.parquet", "zulu zulu");
    table.refresh();
    scope(table, Expressions.equal(LocalTableFixture.COLUMN, "alpha alpha"));

    BuildConfig config = LocalTableFixture.config();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    // "zulu" lives only in the UNINDEXED file. A complete index would prune f1 and keep f2; a
    // narrowed one knows nothing about f2 and must therefore keep it. Either way f2 survives --
    // which is the whole safety argument for scoping, asserted rather than assumed.
    IndexPruner pruner =
        new IndexPruner(new TermIndex(config.format(), new io.kahshe.common.Metrics()), new io.kahshe.common.Metrics(), config.format());
    List<org.apache.iceberg.FileScanTask> tasks = new ArrayList<>();
    try (var planned = table.newScan().planFiles()) {
      planned.forEach(tasks::add);
    }
    assertEquals(2, tasks.size());

    List<org.apache.iceberg.FileScanTask> kept =
        pruner.prune(
            table,
            null,
            List.of(new IndexPruner.ContainsHint(
                LocalTableFixture.COLUMN, "zulu", IndexPruner.HintKind.CONTAINS)),
            tasks);
    assertTrue(
        kept.stream().anyMatch(t -> t.file().location().endsWith("f2.parquet")),
        "a file outside the index scope was pruned away; it holds the only match");
  }

  @Test
  void aMalformedScopeFailsTheBuildInsteadOfSilentlyCoveringEverything() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha");
    table.updateProperties().set(IndexScope.PROPERTY, "{not valid json").commit();
    table.refresh();

    // Both lenient defaults are SAFE in the advisory-keep sense, which is exactly why neither
    // would be noticed: covering everything silently costs hours on a large table, covering
    // nothing silently stops pruning. An operator error has to be loud.
    assertThrows(
        IllegalArgumentException.class,
        () -> IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, LocalTableFixture.config()));
  }

  /**
   * Widening the scope indexes only what the widening added.
   *
   * <p>This is the property that makes scoping a staged strategy rather than a one-shot choice:
   * index seven days now, widen to fourteen later, and the first seven are not read again. It
   * falls out of the incremental gate -- a wider scope is a superset of the covered set, which is
   * exactly the condition the incremental path already tests -- but "it should follow" is not an
   * assertion, so it is asserted.
   */
  @Test
  void wideningTheScopeIndexesOnlyTheFilesTheWideningAdded() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha alpha");
    LocalTableFixture.appendFile(table, "f2.parquet", "bravo bravo");
    LocalTableFixture.appendFile(table, "f3.parquet", "charlie charlie");
    table.refresh();
    BuildConfig config = LocalTableFixture.config();

    // narrow first: one file
    scope(table, Expressions.equal(LocalTableFixture.COLUMN, "alpha alpha"));
    io.kahshe.common.Metrics narrow = new io.kahshe.common.Metrics();
    assertNotNull(
        IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config, null, "p", "ns", "t", narrow));
    assertEquals(1, coveredFiles(table, config).size());
    assertEquals(1, narrow.indexDataFilesRead.sum(), "the narrow build read more than its scope");

    // widen: the two remaining files, and ONLY those
    scope(
        table,
        Expressions.or(
            Expressions.equal(LocalTableFixture.COLUMN, "alpha alpha"),
            Expressions.or(
                Expressions.equal(LocalTableFixture.COLUMN, "bravo bravo"),
                Expressions.equal(LocalTableFixture.COLUMN, "charlie charlie"))));
    io.kahshe.common.Metrics wider = new io.kahshe.common.Metrics();
    assertNotNull(
        IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config, null, "p", "ns", "t", wider));

    assertEquals(3, coveredFiles(table, config).size(), "widening did not extend coverage");
    assertEquals(
        2, wider.indexDataFilesRead.sum(),
        "widening re-read files the narrow build had already indexed");

    // and the index still answers for a term from the ORIGINAL scope, so widening did not lose it
    TermIndex index = new TermIndex(config.format(), new io.kahshe.common.Metrics());
    TermIndex.Loaded loaded = index.forField(
        table, table.schema().findField(LocalTableFixture.COLUMN).fieldId());
    assertNotNull(loaded);
    assertTrue(
        index.entriesFor(table, loaded, List.of("alpha")).containsKey("alpha"),
        "widening the scope dropped a term the earlier, narrower build had indexed");
  }

  /**
   * A scope that adds no files is a no-op, and must report itself as one.
   *
   * <p>Widening kahshe.index.scope to a threshold that happens to select the same files runs in
   * seconds and exits successfully, which is correct -- and easy to read as a failure, because a
   * build with nothing to do reports itself in the same words as a build that did everything.
   *
   * <p>Asserted on the RESULT rather than the log: files covered is unchanged and data bytes read
   * is zero. A regression that silently re-read the covered files would move the second number.
   */
  @Test
  void wideningToAScopeThatAddsNoFilesReadsNothing() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha alpha");
    LocalTableFixture.appendFile(table, "f2.parquet", "zulu zulu");
    table.refresh();
    BuildConfig config = LocalTableFixture.config();

    scope(table, Expressions.equal(LocalTableFixture.COLUMN, "alpha alpha"));
    long[] first = IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    assertNotNull(first);
    assertEquals(1, first[2], "the scoped build should cover one file");
    assertTrue(first[3] > 0, "the first build should have read data");

    // a DIFFERENT expression selecting the SAME file: nothing new to index
    scope(
        table,
        Expressions.or(
            Expressions.equal(LocalTableFixture.COLUMN, "alpha alpha"),
            Expressions.equal(LocalTableFixture.COLUMN, "no-such-value-anywhere")));
    long[] second = IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    assertNotNull(second);
    assertEquals(1, second[2], "coverage changed when the scope added no files");
    assertEquals(0, second[3], "a no-op build read data it did not need to");

  }

  /**
   * A file outside the scope must be KEPT when the query is a token MATCH — the predicate the term
   * tier alone answers.
   *
   * <p>{@code aFileOutsideTheScopeIsKeptRatherThanPruned} reads as though it covers this and does
   * not. Its hint is CONTAINS, and {@code IndexPruner.prune} routes CONTAINS to the gram and bloom
   * layers; only MATCH reaches {@code pruneByTerms}. The term tier's entire out-of-scope guarantee
   * is four lines at the {@code outside index coverage: never prune} keep in
   * {@code TermIndexType.pruneByTerms}, where a file with no ordinal is kept before any bitmap is
   * consulted, and nothing reaches them without a MATCH hint driven through the pruner.
   *
   * <p>That is the one error class this index may never have. The scoped file holds the token; the
   * unscoped one holds it too and the index has never heard of it. Pruning it would drop rows from
   * someone's answer with no exception and no metric — and because advisory-keep normally absorbs
   * exactly this, nothing else would notice.
   */
  @Test
  void aFileOutsideTheScopeIsKeptOnAMatchQueryToo() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha bravo");
    String outside = LocalTableFixture.appendFile(table, "f2.parquet", "alpha charlie");
    table.refresh();
    BuildConfig config = LocalTableFixture.config();

    // index ONLY the first file; f2 holds "alpha" as well and the index will never know
    scope(table, Expressions.equal(LocalTableFixture.COLUMN, "alpha bravo"));
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));
    assertEquals(1, coveredFiles(table, config).size(), "the scoped build covered the wrong count");

    List<org.apache.iceberg.FileScanTask> tasks = LocalTableFixture.planTasks(table);
    assertEquals(2, tasks.size(), "the table should still plan both files");

    IndexPruner pruner = new IndexPruner(new TermIndex(config.format(), new io.kahshe.common.Metrics()),
        new io.kahshe.common.Metrics(), config.format());
    List<IndexPruner.ContainsHint> match =
        List.of(new IndexPruner.ContainsHint(
            LocalTableFixture.COLUMN, "alpha", IndexPruner.HintKind.MATCH));

    List<String> kept =
        pruner.prune(table, null, match, tasks).stream()
            .map(t -> t.file().location())
            .toList();
    assertTrue(
        kept.contains(outside),
        "a file outside the index scope was PRUNED on a MATCH query even though it contains the "
            + "token — the one failure this index may never have");

    // and a token that exists in NO file still prunes the covered one, so the keep above is the
    // guard doing its job rather than the pruner having given up entirely
    List<IndexPruner.ContainsHint> absent =
        List.of(new IndexPruner.ContainsHint(
            LocalTableFixture.COLUMN, "zulu", IndexPruner.HintKind.MATCH));
    List<String> keptAbsent =
        pruner.prune(table, null, absent, tasks).stream()
            .map(t -> t.file().location())
            .toList();
    assertEquals(
        List.of(outside), keptAbsent,
        "pruning stopped working entirely, so the keep above proves nothing");
  }

  private static void scope(Table table, org.apache.iceberg.expressions.Expression expression) {
    table.updateProperties().set(IndexScope.PROPERTY, ExpressionParser.toJson(expression)).commit();
    table.refresh();
  }

  private static List<String> coveredFiles(Table table, BuildConfig config) throws Exception {
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    Path meta =
        Path.of(
            TermIndexWriter.dir(IndexPaths.root(table, config.format().indexRoot()), fieldId)
                + "/index-metadata.json");
    var node =
        TermIndexWriter.snapshotNode(
            new com.fasterxml.jackson.databind.ObjectMapper().readTree(meta.toFile()));
    // Through Coverage, not by reading the node directly: entries carry their own ordinal and
    // liveness, and a hand-rolled f.asText() over that shape returns empty strings rather than
    // failing.
    return Coverage.livePaths(Coverage.parse(node.path("files")));
  }
}
