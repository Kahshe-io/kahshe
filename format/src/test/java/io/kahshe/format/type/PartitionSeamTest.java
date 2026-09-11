package io.kahshe.format.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.Metrics;
import io.kahshe.format.FormatConfig;
import io.kahshe.format.IndexPruner;
import io.kahshe.format.type.bloom.BloomIndexType;
import io.kahshe.format.type.bloom.IndexStore;
import io.kahshe.format.type.gram.GramIndex;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.format.type.term.TermIndexType;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.indexer.build.IndexBuilder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.RoaringBitmap;

/**
 * What the three-way seam buys, asserted where a two-way one could not state it.
 *
 * <p>The whole of {@link IndexType#partition} is one distinction: a file a tier PROVED holds the
 * probe and a file it cannot speak for are different facts, and the {@code List<FileScanTask>}
 * this replaced returned both as the same list entry. Pruning is the one caller for which they
 * coincide — which is why every existing test still passes unchanged and none of them could have
 * caught this being wrong.
 *
 * <p>These do not re-test pruning. The 38 {@code prune(...)} call sites across the suite pin that,
 * and they are deliberately untouched: if this refactor changed which files a plan keeps, they go
 * red, and that is the pin. What is here is the part they cannot see.
 */
class PartitionSeamTest {

  @TempDir Path tmp;

  private static final String NEEDLE = "zzqxy";

  /**
   * The distinction itself, on the shape that produces it: an index built over one file, a second
   * file appended after, and a term that is in neither. The covered file is an ABSENCE — proven,
   * prunable — and the uncovered one is UNKNOWN, and under the old return both were simply
   * missing-or-present in one list with no way to ask which.
   *
   * <p>Red by making {@code TermIndexType.byCoverage} add an uncovered file to {@code hits}
   * instead of leaving it unknown — which is precisely the reading a caller of the old two-way
   * answer was forced into, and it is the fabricated-evidence bug {@code HuntPass} exists to avoid.
   */
  @Test
  void aCoveredMissAndAnUncoveredFileAreDifferentVerdicts() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha");
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    String uncovered = LocalTableFixture.appendFile(table, "later.parquet", "bravo");
    table.refresh();

    Seam seam = new Seam(table, config);
    IndexType.Partition answer = seam.ask(new TermIndexType(), NEEDLE);

    assertEquals(List.of(), seam.pathsIn(answer.hits()), "nothing holds the needle");
    assertEquals(
        List.of(uncovered),
        seam.pathsIn(answer.unknown()),
        "the file appended after the build is not spoken for, and saying so is the point");
    assertEquals(1, answer.absent().getCardinality(), "the covered file is a proven absence");
    assertFalse(
        seam.pathsIn(answer.absent()).contains(uncovered),
        "an uncovered file may never be proved absent -- that is the one forbidden error");
  }

  /**
   * The same table, the same probe, from the other side: the needle IS in the covered file. It
   * becomes a hit, the uncovered file stays unknown, and the two are in different sets — the
   * evidence the old shape computed and then discarded.
   *
   * <p>Red by returning {@code Partition.of(files, absent, hits)} with the arguments swapped: the
   * covered file then reads as absent, which the assertions below both catch.
   */
  @Test
  void aProvenHitIsNotTheSameSetAsAFileTheIndexNeverSaw() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha " + NEEDLE);
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    String covered = pathsOf(LocalTableFixture.planTasks(table)).get(0);
    String uncovered = LocalTableFixture.appendFile(table, "later.parquet", "bravo " + NEEDLE);
    table.refresh();

    Seam seam = new Seam(table, config);
    IndexType.Partition answer = seam.ask(new TermIndexType(), NEEDLE);

    assertEquals(List.of(covered), seam.pathsIn(answer.hits()));
    assertEquals(List.of(uncovered), seam.pathsIn(answer.unknown()));
    assertEquals(0, answer.absent().getCardinality());
    // Both files survive pruning, which is correct and is exactly why pruning could never tell
    // this test's two files apart.
    assertEquals(2, answer.kept().getCardinality(), "kept fuses them; the partition does not");
  }

  /**
   * The bloom tier can prove absence and can never prove presence, so its {@code hits} is empty
   * even for a file that genuinely holds the value. That is not a gap in the implementation, it is
   * the tier's contract ({@code NgramBloom}: a positive "means nothing"), and it is the case that
   * makes two verdicts insufficient — a two-way answer forces the bloom to spell "I cannot say"
   * with the word the exact tiers use for a proof.
   *
   * <p>Red by having {@code BloomIndexType.partition} put its survivors in {@code hits} rather
   * than leaving them unknown.
   */
  @Test
  void theBloomNeverClaimsAHitEvenOnAFileThatHoldsTheValue() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha " + NEEDLE, "bravo");
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    table.refresh();

    Seam seam = new Seam(table, config);
    IndexType.Partition answer = seam.ask(new BloomIndexType(), NEEDLE);

    assertEquals(0, answer.hits().getCardinality(),
        "a filter with no false negatives cannot prove presence, and must not claim to");
    assertTrue(answer.absent().getCardinality() + answer.unknown().getCardinality() > 0);
  }

  /** Every tier's answer is a partition: three disjoint sets that together are exactly what it
   * was handed. The invariant is checked in the constructor, so this asserts the other half —
   * that nothing is dropped on the floor. */
  @Test
  void everyTierAccountsForEveryFileItWasHanded() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha " + NEEDLE, "bravo", "charlie");
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    LocalTableFixture.appendFile(table, "later.parquet", "delta");
    table.refresh();

    Seam seam = new Seam(table, config);
    for (IndexType type : IndexTypes.inCostOrder()) {
      IndexType.Partition answer = seam.ask(type, NEEDLE);
      RoaringBitmap all = RoaringBitmap.or(answer.hits(), answer.absent(), answer.unknown());
      assertEquals(seam.files().inPlay(), all,
          type.key() + " must account for every file it was handed, in exactly one verdict");
    }
  }

  /**
   * The conjunction rule, which is where a partition can go quietly wrong. A clause nobody could
   * answer must collapse the hits — you cannot claim a file satisfies "A and B" having only
   * proved A — while absences still union, because either half ruling a file out rules the pair
   * out.
   */
  @Test
  void anUnanswerableClauseCollapsesTheHitsAndKeepsTheAbsences() {
    IndexType.FileSet files = IndexType.FileSet.of(List.of("a", "b", "c"));
    IndexType.Partition proved =
        IndexType.Partition.of(files, RoaringBitmap.bitmapOf(0), RoaringBitmap.bitmapOf(1));
    IndexType.Partition nothingKnown = IndexType.Partition.allUnknown(files);

    IndexType.Partition both = proved.and(nothingKnown);
    assertEquals(0, both.hits().getCardinality(),
        "half a conjunction proved is not the conjunction proved");
    assertEquals(RoaringBitmap.bitmapOf(1), both.absent(),
        "the half that DID prove an absence still proves the pair absent");
    assertEquals(RoaringBitmap.bitmapOf(0, 2), both.unknown());
    // and the caller's projection is unchanged: only absences ever cost a file its place
    assertEquals(RoaringBitmap.bitmapOf(0, 2), both.kept());
  }

  /** Two verdicts for one file is two tiers disagreeing about a fact, not a slower plan; the
   * record refuses to exist rather than let a caller be right by accident. */
  @Test
  void aFileCannotCarryTwoVerdicts() {
    assertThrows(IllegalStateException.class,
        () -> new IndexType.Partition(
            RoaringBitmap.bitmapOf(0), RoaringBitmap.bitmapOf(0), new RoaringBitmap()));
  }

  /**
   * Plan ordinals are positions, not paths, and Iceberg may hand the same data file over as more
   * than one scan task. Both positions must get the same verdict and both must come back — a
   * path-keyed answer would have collapsed them into one.
   */
  @Test
  void twoTasksOverOnePathAreTwoOrdinalsWithTheSameVerdict() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha " + NEEDLE);
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    table.refresh();

    List<FileScanTask> once = LocalTableFixture.planTasks(table);
    assertEquals(1, once.size(), "precondition: one task to duplicate");
    List<FileScanTask> twice = new ArrayList<>(once);
    twice.addAll(once);

    Seam seam = new Seam(table, config);
    IndexType.Partition answer =
        seam.ask(new TermIndexType(), NEEDLE, IndexType.FileSet.of(pathsOf(twice)));
    assertEquals(RoaringBitmap.bitmapOf(0, 1), answer.hits(),
        "the same file offered twice is two plan ordinals, and both hold the term");
  }

  private static List<String> pathsOf(List<FileScanTask> tasks) {
    List<String> paths = new ArrayList<>(tasks.size());
    for (FileScanTask task : tasks) {
      paths.add(task.file().location());
    }
    return paths;
  }

  /** The seam's inputs, assembled the way {@code IndexPruner} assembles them, so these tests
   * drive the real readers rather than a stand-in. */
  private final class Seam {
    private final Table table;
    private final FormatConfig format;
    private final IndexType.ReadContext read;
    private final IndexType.FileSet files;

    Seam(Table table, BuildConfig config) throws Exception {
      this.table = table;
      this.format = config.format();
      Metrics metrics = new Metrics();
      this.read = new IndexType.ReadContext(
          table, format, metrics, new IndexStore(format, metrics), new GramIndex(format, metrics),
          new TermIndex(format, metrics));
      this.files = IndexType.FileSet.of(pathsOf(LocalTableFixture.planTasks(table)));
    }

    IndexType.FileSet files() {
      return files;
    }

    IndexType.Partition ask(IndexType type, String needle) {
      return ask(type, needle, files);
    }

    IndexType.Partition ask(IndexType type, String needle, IndexType.FileSet over) {
      IndexPruner.ContainsHint hint =
          new IndexPruner.ContainsHint(
              LocalTableFixture.COLUMN, needle, IndexPruner.HintKind.MATCH);
      // The bloom and gram tiers read candidates rather than hints, so the probe carries the same
      // question in both spellings -- which is what IndexPruner.prune does for a CONTAINS.
      IndexType.Probe probe = new IndexType.Probe(
          table,
          List.of(new IndexPruner.Candidate(
              LocalTableFixture.COLUMN, List.of(needle),
              io.kahshe.format.type.bloom.NgramBloom.Mode.CONTAINS)),
          List.of(),
          List.of(hint),
          format,
          new IndexType.GramProbes());
      IndexType.Loaded loaded = type.load(read);
      return loaded == null
          ? IndexType.Partition.allUnknown(over)
          : type.partition(loaded, probe, over);
    }

    List<String> pathsIn(RoaringBitmap ordinals) {
      List<String> paths = new ArrayList<>();
      for (int ordinal : ordinals) {
        paths.add(files.pathOf(ordinal));
      }
      return paths;
    }
  }
}
