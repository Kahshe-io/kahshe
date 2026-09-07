package io.kahshe.indexer.build;

import io.kahshe.format.Coverage;
import io.kahshe.format.IndexPaths;
import io.kahshe.format.IndexPruner;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.format.type.term.TermIndexWriter;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kahshe.common.Metrics;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;

/**
 * Tombstones make a departure cheap; compaction is what stops them accumulating forever.
 *
 * <p>A table on rolling retention eventually carries coverage that is almost entirely dead, plus
 * aggregate rows whose only file is gone. Past a threshold a build renumbers the survivors
 * contiguously and drops the rest — but that is only sound if EVERY bitmap in the gram and term
 * tiers is translated in the same build. Coverage and bitmaps that disagree about what a number
 * means is the exact false negative this format exists to prevent, so the test that matters is not
 * "did it compact" but "does it still prune to the right file afterwards".
 */
class TombstoneCompactionTest {
  @TempDir Path tmp;

  private static int fieldId(Table table) {
    return table.schema().findField(LocalTableFixture.COLUMN).fieldId();
  }

  private static List<Coverage.Entry> coverageOf(Table table, BuildConfig config)
      throws Exception {
    Path meta =
        Path.of(
            TermIndexWriter.dir(IndexPaths.root(table, config.format().indexRoot()), fieldId(table))
                + "/index-metadata.json");
    return Coverage.parse(
        TermIndexWriter.snapshotNode(new ObjectMapper().readTree(meta.toFile())).path("files"));
  }

  private static List<String> keptFor(
      Table table, BuildConfig config, String token, IndexPruner.HintKind kind) throws Exception {
    Metrics metrics = new Metrics();
    IndexPruner pruner = new IndexPruner(new TermIndex(config.format(), metrics), metrics, config.format());
    return pruner
        .prune(
            table,
            null,
            List.of(new IndexPruner.ContainsHint(LocalTableFixture.COLUMN, token, kind)),
            LocalTableFixture.planTasks(table))
        .stream()
        .map(t -> t.file().location())
        .toList();
  }

  /**
   * The whole feature, and the assertion that makes it safe.
   *
   * <p>Twenty files are indexed, seventeen leave at once — past both the dead fraction and the
   * absolute floor — and the build compacts. The survivors keep their DATA but not their numbers,
   * so a term index that failed to translate its bitmaps would now resolve a surviving file's token
   * to whichever file inherited its old ordinal.
   *
   * <p>Verified by breaking it: passing {@code null} instead of {@code ordinalRemap} into
   * {@code TermIndexWriter.finish} leaves the aggregate in the old numbering and the MATCH
   * assertions below fail, each naming the wrong file.
   */
  @Test
  void compactionRenumbersSurvivorsWithoutRepointingASingleBitmap() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "marker0 shared");
    List<String> paths = new ArrayList<>();
    // from f2: createTable already wrote f1.parquet, and re-appending that name overwrites it
    for (int i = 2; i <= 20; i++) {
      paths.add(LocalTableFixture.appendFile(table, "f" + i + ".parquet", "marker" + i + " shared"));
    }
    table.refresh();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));
    assertEquals(20, coverageOf(table, config).size(), "precondition: twenty files covered");

    // the survivors: the original file plus the last two appended
    String keepA = paths.get(paths.size() - 1);
    String keepB = paths.get(paths.size() - 2);
    int keepAMarker = 20;
    int keepBMarker = 19;

    for (int i = 0; i < paths.size() - 2; i++) {
      table.newDelete().deleteFile(paths.get(i)).commit();
    }
    table.refresh();
    // one new file, so the build has something to do and takes the incremental path
    String fresh = LocalTableFixture.appendFile(table, "fresh.parquet", "freshmarker shared");
    table.refresh();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    List<Coverage.Entry> after = coverageOf(table, config);
    assertFalse(
        Coverage.hasTombstones(after),
        "compaction should have dropped every dead entry, leaving: " + after);
    assertEquals(4, after.size(), "expected the three survivors plus the new file: " + after);
    // renumbered contiguously from zero, which is the point of compacting
    List<Integer> ordinals = after.stream().map(Coverage.Entry::ordinal).sorted().toList();
    assertEquals(List.of(0, 1, 2, 3), ordinals, "survivors were not renumbered contiguously");

    // THE ASSERTION THAT MATTERS. Every surviving file's tokens must still resolve to that file
    // and no other. A bitmap left in the old numbering would point somewhere else entirely.
    assertEquals(List.of(keepA), keptFor(table, config, "marker" + keepAMarker, IndexPruner.HintKind.MATCH));
    assertEquals(List.of(keepB), keptFor(table, config, "marker" + keepBMarker, IndexPruner.HintKind.MATCH));
    assertEquals(List.of(fresh), keptFor(table, config, "freshmarker", IndexPruner.HintKind.MATCH));
    // a token every surviving file holds must keep them all -- the over-pruning control
    assertEquals(4, keptFor(table, config, "shared", IndexPruner.HintKind.MATCH).size());
    // and a token whose only file left prunes to nothing rather than to somebody else's file
    assertEquals(List.of(), keptFor(table, config, "marker3", IndexPruner.HintKind.MATCH));

    // the gram tier is ordinal-keyed too, and its answer is FINAL inside its coverage
    assertEquals(
        List.of(keepA), keptFor(table, config, "marker" + keepAMarker, IndexPruner.HintKind.CONTAINS));
  }

  /**
   * Files leave and NOTHING new arrives — the shape rolling retention actually has on a quiet day.
   *
   * <p>This is where compaction is easiest to get unsound, and the trap is worth stating because
   * the guard against it looks like a one-line reordering and is not. Compaction is decided BEFORE
   * the build works out whether it has any new files to read. If it has none, the build takes a
   * restamp shortcut that rewrites only the metadata document — so a compacting build on that path
   * would publish RENUMBERED COVERAGE against UNTRANSLATED BITMAPS. Coverage would say ordinal 3
   * is this file
   * while the aggregate still meant the file that used to hold 3: a live path resolving to another
   * file's data, which is the exact false negative the ordinal format exists to prevent.
   *
   * <p>A build that compacts must therefore go through the merge, which is what translates the
   * bitmaps, even though it has nothing new to read.
   *
   * <p>Verified by breaking it: dropping {@code && ordinalRemap == null} from the restamp
   * condition puts the shortcut back and fails the MATCH assertions below.
   */
  @Test
  void compactionOnADeleteOnlyBuildStillTranslatesTheBitmaps() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "marker0 shared");
    List<String> paths = new ArrayList<>();
    for (int i = 2; i <= 20; i++) {
      paths.add(LocalTableFixture.appendFile(table, "f" + i + ".parquet", "marker" + i + " shared"));
    }
    table.refresh();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    String keep = paths.get(paths.size() - 1);
    // delete everything except the original file and the last one -- and add NOTHING
    for (int i = 0; i < paths.size() - 1; i++) {
      table.newDelete().deleteFile(paths.get(i)).commit();
    }
    table.refresh();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    List<Coverage.Entry> after = coverageOf(table, config);
    assertFalse(Coverage.hasTombstones(after), "compaction should have run: " + after);
    assertEquals(2, after.size(), "expected the two survivors: " + after);

    // the survivors' tokens must still resolve to the survivors, not to whoever inherited their
    // old ordinals
    assertEquals(List.of(keep), keptFor(table, config, "marker20", IndexPruner.HintKind.MATCH));
    assertEquals(2, keptFor(table, config, "shared", IndexPruner.HintKind.MATCH).size());
    assertEquals(List.of(), keptFor(table, config, "marker5", IndexPruner.HintKind.MATCH));
  }

  /**
   * Counting must stay refused across compaction, and this is the subtle one.
   *
   * <p>Compaction removes the tombstones but cannot remove the departed files' occurrences from
   * {@code total_count}, which is a scalar sum. A flag keyed on "has tombstones" would therefore
   * flip back to exact the moment a build compacted, and {@code _count} would resume answering with
   * an inflated number while labelling it {@code "exact": true}. The flag is sticky instead.
   *
   * <p>Verified by breaking it: deriving the flag from {@code Coverage.hasTombstones} rather than
   * carrying it forward makes this fail immediately after compaction.
   */
  @Test
  void countsStayInexactAfterCompactionHasRemovedTheEvidence() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "marker0 shared");
    List<String> paths = new ArrayList<>();
    // from f2: createTable already wrote f1.parquet, and re-appending that name overwrites it
    for (int i = 2; i <= 20; i++) {
      paths.add(LocalTableFixture.appendFile(table, "f" + i + ".parquet", "marker" + i + " shared"));
    }
    table.refresh();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));
    assertTrue(
        new TermIndex(config.format(), new Metrics()).forField(table, fieldId(table)).countsExact(),
        "precondition: nothing has left yet");

    for (int i = 0; i < paths.size() - 2; i++) {
      table.newDelete().deleteFile(paths.get(i)).commit();
    }
    table.refresh();
    LocalTableFixture.appendFile(table, "fresh.parquet", "freshmarker shared");
    table.refresh();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    assertFalse(
        Coverage.hasTombstones(coverageOf(table, config)),
        "precondition: compaction ran, so there is no tombstone left to key a flag on");
    assertFalse(
        new TermIndex(config.format(), new Metrics()).forField(table, fieldId(table)).countsExact(),
        "compaction erased the tombstones and the index went back to claiming exact counts; "
            + "_count would now report an upper bound as exact");
  }
}
