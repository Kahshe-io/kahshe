package io.kahshe.indexer.maintain;

import io.kahshe.analysis.analyzer.Analyzer;
import io.kahshe.format.IndexPaths;
import io.kahshe.format.IndexPruner;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.format.type.term.TermIndexWriter;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.Metrics;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.indexer.build.IndexBuilder;

/**
 * A reader applies the admission rule of the analyzer the INDEX names, so the previous
 * generation stays readable while its rebuild runs (N-1). v1 excluded pure-numeric tokens over
 * four digits; a v1 index is read under that rule — probed for what v1 wrote, kept whole for what
 * it did not — rather than refused.
 *
 * <p>The v1 index is simulated: a build under a four-character cap (which likewise leaves
 * {@code 12345} unwritten) has its analyzer id patched to v1, with the {@code .crc} sidecar
 * deleted so the local file system does not reject the patched file.
 *
 * <p>Verified red three times: with v1 refused again ({@code contractOf} answering null), the first
 * assertion keeps both files; with v1 read as a capless v2 (the rule dropped), the second prunes
 * both — a probe for a token v1 never wrote finds it absent; with {@code indexCurrent}'s analyzer
 * clause removed, the patched index is reported current and never rebuilt.
 */
class AnalyzerGenerationTest {
  @TempDir Path tmp;

  private static Set<String> kept(Table table, BuildConfig config, String token)
      throws Exception {
    Metrics metrics = new Metrics();
    IndexPruner pruner = new IndexPruner(new TermIndex(config.format(), metrics), metrics, config.format());
    return pruner
        .prune(
            table, null,
            List.of(new IndexPruner.ContainsHint(LocalTableFixture.COLUMN, token, IndexPruner.HintKind.MATCH)),
            LocalTableFixture.planTasks(table))
        .stream().map(t -> t.file().location()).collect(Collectors.toSet());
  }

  @Test
  void theV1RuleIsKeptVerbatimAndUnknownFamiliesAreStillRefused() {
    Analyzer.Contract v1 = Analyzer.contractOf(Analyzer.V1_ID);
    assertEquals(1, v1.version());
    assertFalse(v1.isIndexable("12345"), "five digits: excluded by v1");
    assertTrue(v1.isIndexable("1234"), "four digits: admitted");
    assertTrue(v1.isIndexable("abc12345"), "a letter anywhere: admitted");
    assertTrue(v1.isIndexable("x".repeat(5000)), "v1 had no length cap");
    Analyzer.Contract v2 = Analyzer.contractOf(Analyzer.ID_PREFIX + "4");
    assertEquals(2, v2.version());
    assertEquals(3, Analyzer.contractOf((Analyzer.V3_ID_PREFIX + 4)).version(), "a build writes v3 now");
    assertTrue(v2.isIndexable("1234"));
    assertFalse(v2.isIndexable("abcde"), "v2 is length only");
    assertNull(Analyzer.contractOf("kahshe-ascii-v0"), "an unknown family is refused");
    assertNull(Analyzer.contractOf(null));
  }

  @Test
  void aV1IndexIsReadUnderV1sRuleInsteadOfRefused() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "abc 12345");
    String f1 = LocalTableFixture.planTasks(table).get(0).file().location();
    String f2 = LocalTableFixture.appendFile(table, "f2.parquet", "xyz 777");
    table.updateProperties()
        .set("kahshe.index." + LocalTableFixture.COLUMN + ".max-token-length", "4")
        .commit();
    table.refresh();
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);

    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    Path meta =
        Path.of(
            TermIndexWriter.dir(IndexPaths.root(table, config.format().indexRoot()), fieldId)
                + "/index-metadata.json");
    String json = Files.readString(meta);
    assertTrue(json.contains((Analyzer.V3_ID_PREFIX + 4)), "built under the four-character cap");
    IndexerService indexer =
        new IndexerService(LocalTableFixture.noTables(), new Metrics(), false, config);
    long snapshot = table.currentSnapshot().snapshotId();
    assertTrue(indexer.indexCurrent(table, LocalTableFixture.COLUMN, snapshot), "current as built");
    Files.writeString(meta, json.replace((Analyzer.V3_ID_PREFIX + 4), Analyzer.V1_ID));
    Files.deleteIfExists(meta.resolveSibling("." + meta.getFileName() + ".crc"));
    assertFalse(
        indexer.indexCurrent(table, LocalTableFixture.COLUMN, snapshot),
        "an index under another analyzer is not current: the indexer rebuilds it while it serves");

    TermIndex.Loaded loaded = new TermIndex(config.format(), new Metrics()).forField(table, fieldId);
    assertNotNull(loaded, "the v1 index loads instead of being refused");
    assertEquals(1, loaded.contract().version());
    assertEquals(Set.of(f1), kept(table, config, "abc"), "a token v1 wrote prunes");
    assertEquals(Set.of(f2), kept(table, config, "777"), "three digits: v1 wrote it");
    assertEquals(
        Set.of(f1, f2),
        kept(table, config, "12345"),
        "five digits: v1 never wrote it, so it is not probed and every file is kept");
  }
}
