package io.kahshe.indexer.build;

import io.kahshe.analysis.analyzer.Analyzer;
import io.kahshe.format.IndexPruner;
import io.kahshe.format.type.term.TermIndex;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.Metrics;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;

/**
 * {@code kahshe.index.<column>.analyzer = value}: the canonical value is ONE term, exact and
 * case-sensitive, so plain {@code =}/{@code IN} on the column — the no-plugin path — prunes
 * through the term tier where the gram tier saturates (dotted decimals) and the tokens analyzer
 * splits (octets). The substring tiers are untouched: {@code contains} still works.
 *
 * <p>Verified red twice: with the pruner's value probe skipped, the case-mismatched equality is
 * kept by the gram tier (which lowercases) instead of pruned; with the contract's whole-value
 * branch dropped, a {@code match} hint tokenizes the address into octets and finds no term.
 */
class WholeValueAnalyzerTest {
  @TempDir Path tmp;

  private static Set<String> kept(Table table, BuildConfig config, Expression filter,
      List<IndexPruner.ContainsHint> hints) throws Exception {
    Metrics metrics = new Metrics();
    IndexPruner pruner = new IndexPruner(new TermIndex(config.format(), metrics), metrics, config.format());
    return pruner.prune(table, filter, hints, LocalTableFixture.planTasks(table)).stream()
        .map(t -> t.file().location())
        .collect(Collectors.toSet());
  }

  @Test
  void equalityOnAWholeValueColumnPrunesExactlyAndCaseSensitively() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "10.0.0.1");
    String f1 = LocalTableFixture.planTasks(table).get(0).file().location();
    String f2 = LocalTableFixture.appendFile(table, "f2.parquet", "10.0.1.0.1");
    String f3 = LocalTableFixture.appendFile(table, "f3.parquet", "Abc");
    table.updateProperties()
        .set("kahshe.index." + LocalTableFixture.COLUMN + ".analyzer", "value")
        .commit();
    table.refresh();
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);

    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    TermIndex.Loaded loaded = new TermIndex(config.format(), new Metrics()).forField(table, fieldId);
    assertEquals(Analyzer.Kind.VALUE, loaded.contract().kind());
    assertEquals(Analyzer.VALUE_ID_PREFIX + config.maxTokenLength(), loaded.analyzer());

    String c = LocalTableFixture.COLUMN;
    assertEquals(Set.of(f1), kept(table, config, Expressions.equal(c, "10.0.0.1"), List.of()),
        "the whole address is one term: the file whose octets merely repeat it is pruned");
    assertEquals(Set.of(f1, f3), kept(table, config, Expressions.in(c, "10.0.0.1", "Abc"), List.of()),
        "IN is the union per literal");
    // Trino pushes `col = 'x'` down as an IN with one value. Iceberg's unbound IN refuses
    // literal() ("IN predicate cannot return a literal"), so a range collector that asks for the
    // literal before looking at the operator answers 400 to every equality pushed down through
    // Trino. Verified red against a collector that does.
    assertEquals(Set.of(f1), kept(table, config, Expressions.in(c, "10.0.0.1"), List.of()),
        "a single-value IN is an equality, and must never throw");
    assertEquals(Set.of(f3), kept(table, config, Expressions.equal(c, "Abc"), List.of()));
    assertEquals(Set.of(), kept(table, config, Expressions.equal(c, "abc"), List.of()),
        "exact means case-sensitive, as SQL = is: no file can hold the lowercase spelling");
    assertEquals(
        Set.of(f1),
        kept(table, config, null,
            List.of(new IndexPruner.ContainsHint(c, "10.0.0.1", IndexPruner.HintKind.MATCH))),
        "a match hint is the whole value under this contract, not its octets");
    assertTrue(
        kept(table, config, null,
            List.of(new IndexPruner.ContainsHint(c, "0.0.1", IndexPruner.HintKind.CONTAINS)))
            .contains(f1),
        "the substring tiers are unchanged");
    assertEquals(List.of(), loaded.contract().tokens(""),
        "the empty value has no term: the build writes none and the pruner probes none");
  }
}
