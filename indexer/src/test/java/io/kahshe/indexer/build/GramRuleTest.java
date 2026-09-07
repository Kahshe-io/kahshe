package io.kahshe.indexer.build;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.Metrics;
import io.kahshe.common.Records;
import io.kahshe.format.type.bloom.IndexMeta;
import io.kahshe.format.IndexPaths;
import io.kahshe.format.IndexPruner;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.format.type.term.TermIndexWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.indexer.maintain.IndexerService;

/**
 * The gram rule end to end: grams are cut by whole code points, the rule and size are recorded
 * in both tiers' metadata and honoured by both readers, an index under another rule is not
 * current, and the size resolves per column with an out-of-range value falling back.
 *
 * <p>Verified red three ways: the readers ignoring the recorded rule (the relabelled index kept
 * probing under v2), the currency check without the gram comparison (the relabelled index stayed
 * current), and the settings resolver ignoring the property (the size stayed at the default).
 */
class GramRuleTest {
  private static final String MIXED = "ab😀cd";
  @TempDir Path tmp;

  private static Set<String> kept(Table table, BuildConfig config, String literal) throws Exception {
    Metrics metrics = new Metrics();
    IndexPruner pruner = new IndexPruner(new TermIndex(config.format(), metrics), metrics, config.format());
    return pruner.prune(table, null,
            List.of(new IndexPruner.ContainsHint(LocalTableFixture.COLUMN, literal, IndexPruner.HintKind.CONTAINS)),
            LocalTableFixture.planTasks(table))
        .stream().map(t -> t.file().location()).collect(Collectors.toSet());
  }

  private static int fieldId(Table table) {
    return table.schema().findField(LocalTableFixture.COLUMN).fieldId();
  }

  private static Path termMeta(Table table, BuildConfig config) {
    return Path.of(TermIndexWriter.dir(IndexPaths.root(table, config.format().indexRoot()), fieldId(table))
        + "/index-metadata.json");
  }

  private static Path bloomMeta(Table table, BuildConfig config) {
    return Path.of(IndexMeta.metaPath(IndexPaths.root(table, config.format().indexRoot()), fieldId(table)));
  }

  private static void rewrite(Path meta, String from, String to) throws Exception {
    String json = Files.readString(meta);
    assertTrue(json.contains(from), meta + " should name " + from);
    Files.writeString(meta, json.replace(from, to));
    Files.deleteIfExists(meta.resolveSibling("." + meta.getFileName() + ".crc"));
  }

  @Test
  void codePointGramsPruneAndKeepThroughThePruner() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, MIXED);
    String plain = LocalTableFixture.appendFile(table, "f2.parquet", "plain ascii text");
    BuildConfig config = LocalTableFixture.config();
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);

    Set<String> kept = kept(table, config, "b😀c");
    assertEquals(1, kept.size(), "the file holding the emoji is kept, the plain one pruned");
    assertFalse(kept.contains(plain));
    assertTrue(kept(table, config, "😀xy").isEmpty(), "a gram no file holds prunes both");
    assertEquals(2, kept(table, config, "😀x").size(),
        "two code points are shorter than a window: unprobeable, every file kept");
    assertTrue(Files.readString(termMeta(table, config)).contains("kahshe-grams-v2-n3"));
    assertTrue(Files.readString(bloomMeta(table, config)).contains("kahshe-grams-v2-n3"));
  }

  @Test
  void bothReadersProbeUnderTheRuleTheMetadataNames() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, MIXED);
    BuildConfig config = LocalTableFixture.config();
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    BuildConfig bloomOnly = Records.with(config, Map.of("gramIndexEnabled", false));
    assertEquals(1, kept(table, config, "b😀c").size(), "gram tier, as built");
    assertEquals(1, kept(table, bloomOnly, "b😀c").size(), "bloom tier, as built");

    // Relabel both documents as v1. The grams were cut under v2, so a reader that honours the
    // label now asks for windows v2 never wrote and prunes; a reader that ignores it keeps.
    rewrite(termMeta(table, config), "kahshe-grams-v2-n3", "kahshe-grams-v1");
    rewrite(bloomMeta(table, config), "kahshe-grams-v2-n3", "kahshe-grams-v1");
    assertTrue(kept(table, config, "b😀c").isEmpty(), "the gram reader probed under the recorded rule");
    assertTrue(kept(table, bloomOnly, "b😀c").isEmpty(), "the bloom reader probed under the recorded rule");
  }

  @Test
  void anIndexUnderAnotherGramRuleIsNotCurrent() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "hello world");
    BuildConfig config = LocalTableFixture.config();
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    IndexerService indexer = new IndexerService(LocalTableFixture.noTables(), new Metrics(), false, config);
    long snapshot = table.currentSnapshot().snapshotId();
    assertTrue(indexer.indexCurrent(table, LocalTableFixture.COLUMN, snapshot), "current as built");
    rewrite(termMeta(table, config), "kahshe-grams-v2-n3", "kahshe-grams-v2-n4");
    assertFalse(indexer.indexCurrent(table, LocalTableFixture.COLUMN, snapshot),
        "another gram rule or size is not current: the indexer rebuilds it while it serves");
  }

  @Test
  void theSizeResolvesPerColumnThenDeploymentAndOutOfRangeFallsBack() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "hello world");
    BuildConfig config = LocalTableFixture.config();
    table.updateProperties().set("kahshe.index." + LocalTableFixture.COLUMN + ".ngram", "4").commit();
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    assertTrue(Files.readString(termMeta(table, config)).contains("kahshe-grams-v2-n4"));
    assertTrue(Files.readString(bloomMeta(table, config)).contains("\"4\""), "the bloom document records the size");
    assertEquals(1, kept(table, config, "lo w").size(), "a four-gram present keeps");
    assertTrue(kept(table, config, "zzzz").isEmpty(), "a four-gram absent prunes");
    assertEquals(1, kept(table, config, "owo").size(),
        "a literal shorter than a window cannot be probed and keeps every file: the size's trade");

    table.updateProperties().set("kahshe.index." + LocalTableFixture.COLUMN + ".ngram", "12").commit();
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    assertTrue(Files.readString(termMeta(table, config)).contains("kahshe-grams-v2-n3"),
        "out of range falls back to the deployment default, loudly");

    table.updateProperties().remove("kahshe.index." + LocalTableFixture.COLUMN + ".ngram").commit();
    BuildConfig five = Records.with(config, Map.of("ngram", 5));
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, five);
    assertTrue(Files.readString(termMeta(table, five)).contains("kahshe-grams-v2-n5"), "the deployment default applies");
  }
}
