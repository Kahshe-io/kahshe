package io.kahshe.indexer.build;

import io.kahshe.format.type.gram.Grams;
import io.kahshe.format.type.gram.GramIndex;
import io.kahshe.format.IndexPruner;
import io.kahshe.format.type.bloom.NgramBloom;
import io.kahshe.format.type.term.TermIndex;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.Metrics;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;

/**
 * Context-sensitive lowercasing must not prune a truly-matching file: values are lowercased
 * whole, so "ΝΑΣΟΣ" grams with a medial sigma ("νασ"...), while the literal "ΝΑΣ" lowercases
 * standalone to a final sigma ("νας"). Both probe forms must be tried, on both index paths.
 */
class SigmaProbeTest {
  @TempDir Path tmp;

  @Test
  void bloomProbeKeepsInteriorSigmaLiteral() {
    NgramBloom bloom = NgramBloom.build(NgramBloom.gramsOf("ΝΑΣΟΣ", Grams.Contract.current(3)), Grams.Contract.current(3), 0.01);
    assertTrue(bloom.mightContain("ΝΑΣ", NgramBloom.Mode.CONTAINS));
    assertTrue(bloom.mightContain("ΝΑΣΟΣ", NgramBloom.Mode.EQ));
  }

  @Test
  void variantsCoverStandaloneAndMedialForms() {
    assertEquals(List.of("νας", "νασ"), NgramBloom.variants("ΝΑΣ"));
    assertEquals(List.of("abc"), NgramBloom.variants("ABC"));
  }

  /**
   * The mirror image of the case above, and the harder half of {@code variants} to get right.
   *
   * <p>Java's Final_Sigma rule reads the character BEFORE the sigma as well as the one after, so a
   * literal cut at a word-final sigma needs a LEADING sentinel to reproduce how it lowercased in
   * context. "ΠΡΟΣ ΤΟΝ" indexes as "προς τον" (final sigma), while "Σ ΤΟΝ" standing alone lowercases
   * to "σ τον" (non-final) — a form that appears nowhere in the indexed value. Probing only that
   * form finds no gram, and the gram layer is EXACT over its coverage, so it treats the absence as
   * proof and prunes a file that genuinely contains the literal.
   *
   * <p>Verified the way this suite requires: dropping the leading-sentinel form from
   * {@code variants} turns every assertion here red.
   */
  @Test
  void leadingContextSigmaLiteralIsProbedOnEveryPath() throws Exception {
    String value = "ΠΡΟΣ ΤΟΝ";
    String literal = "Σ ΤΟΝ";
    // the premise: this is a true substring match, so pruning the file would lose a real row
    assertTrue(value.contains(literal));
    assertTrue(NgramBloom.variants(literal).contains("ς τον"));

    assertTrue(NgramBloom.build(NgramBloom.gramsOf(value, Grams.Contract.current(3)), Grams.Contract.current(3), 0.01)
        .mightContain(literal, NgramBloom.Mode.CONTAINS));

    Table table = LocalTableFixture.createTable(tmp, value);
    BuildConfig config = LocalTableFixture.config();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    assertTrue(new GramIndex(config.format(), new Metrics())
        .forField(table, fieldId)
        .matches(literal, NgramBloom.Mode.CONTAINS)
        .contains(0));

    Metrics metrics = new Metrics();
    IndexPruner pruner = new IndexPruner(new TermIndex(config.format(), metrics), metrics, config.format());
    List<IndexPruner.ContainsHint> hints = List.of(
        new IndexPruner.ContainsHint(LocalTableFixture.COLUMN, literal, IndexPruner.HintKind.CONTAINS));
    assertEquals(1, pruner.prune(table, null, hints, LocalTableFixture.planTasks(table)).size());
  }

  @Test
  void gramLayerKeepsInteriorSigmaLiteral() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "ΝΑΣΟΣ");
    BuildConfig config = LocalTableFixture.config();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    GramIndex.Loaded loaded = new GramIndex(config.format(), new Metrics()).forField(table, fieldId);
    assertNotNull(loaded);
    assertTrue(loaded.matches("ΝΑΣ", NgramBloom.Mode.CONTAINS).contains(0));
    // still exact for genuinely absent literals
    assertTrue(loaded.matches("ΞΥΖΩ", NgramBloom.Mode.CONTAINS).isEmpty());
  }

  @Test
  void prunerKeepsTheFileOnBothPaths() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "ΝΑΣΟΣ");
    List<IndexPruner.ContainsHint> hints = List.of(
        new IndexPruner.ContainsHint(LocalTableFixture.COLUMN, "ΝΑΣ", IndexPruner.HintKind.CONTAINS));

    // gram path
    BuildConfig gramConfig = LocalTableFixture.config();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, gramConfig));
    Metrics metrics = new Metrics();
    IndexPruner gramPruner = new IndexPruner(new TermIndex(gramConfig.format(), metrics), metrics, gramConfig.format());
    List<FileScanTask> tasks = LocalTableFixture.planTasks(table);
    assertEquals(1, gramPruner.prune(table, null, hints, tasks).size());

    // bloom path (gram layer disabled at serve time)
    BuildConfig bloomConfig = LocalTableFixture.config(false);
    IndexPruner bloomPruner =
        new IndexPruner(new TermIndex(bloomConfig.format(), metrics), metrics, bloomConfig.format());
    assertEquals(1, bloomPruner.prune(table, null, hints, tasks).size());
  }
}
