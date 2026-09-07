package io.kahshe.indexer.build;

import io.kahshe.format.IndexPruner;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.format.type.term.TermIndexWriter;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.kahshe.common.Metrics;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;

/**
 * An analyzer change (a raised token cap, or a v1 index meeting its first v2 build) must force a
 * FULL rebuild, never an incremental restamp.
 *
 * <p>{@code TermIndexWriter.finish} stamps the current analyzer id on every publish and the reader
 * takes the cap from the index. So an incremental build over an index built under a smaller cap
 * would relabel it as the larger cap's while the old files' tokens between the two caps were never
 * written; a {@code match} on such a token would find it absent and prune exactly the files that
 * hold it — the forbidden class, silent. Verified red with the {@code analyzerChanged} clause
 * removed from {@code IndexBuilder}: the build takes the incremental path and the first file is
 * pruned.
 */
class AnalyzerRestampTest {
  @TempDir Path tmp;

  private static Set<String> keptForMatch(Table table, BuildConfig config, String token)
      throws Exception {
    Metrics metrics = new Metrics();
    IndexPruner pruner = new IndexPruner(new TermIndex(config.format(), metrics), metrics, config.format());
    return pruner
        .prune(
            table,
            null,
            List.of(
                new IndexPruner.ContainsHint(
                    LocalTableFixture.COLUMN, token, IndexPruner.HintKind.MATCH)),
            LocalTableFixture.planTasks(table))
        .stream()
        .map(t -> t.file().location())
        .collect(Collectors.toSet());
  }

  @Test
  void raisingTheCapRebuildsInFullSoOldFilesLongerTokensBecomeFindable() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    String cap = "kahshe.index." + LocalTableFixture.COLUMN + ".max-token-length";
    // f1 holds a 12-character token; built under a cap of 8 it is NOT indexed.
    Table table = LocalTableFixture.createTable(tmp, "alpha twelvechars");
    table.updateProperties().set(cap, "8").commit();
    table.refresh();
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    String f1 = LocalTableFixture.planTasks(table).get(0).file().location();
    assertEquals(
        Set.of(f1),
        keptForMatch(table, config, "twelvechars"),
        "control: over the cap the token is unprobeable and the file is kept");

    // Raise the cap and append a file: the next build must re-read f1, not restamp it.
    table.updateProperties().set(cap, "64").commit();
    String f2 = LocalTableFixture.appendFile(table, "f2.parquet", "bravo");
    table.refresh();
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);

    assertEquals(
        Set.of(f1),
        keptForMatch(table, config, "twelvechars"),
        "after the cap is raised, f1's long token is indexed and f1 is kept while f2 is pruned; "
            + "an incremental restamp would have pruned f1 — the forbidden class");
    assertEquals(Set.of(f2), keptForMatch(table, config, "bravo"), "the new file is indexed too");
  }
}
