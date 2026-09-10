package io.kahshe.watch.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.Metrics;
import io.kahshe.format.IndexPruner;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.indexer.build.IndexBuilder;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A hunt's one job is to keep three verdicts apart that the pruner folds into one, and to refuse
 * where the pruner keeps every file. Each test here names the failure it catches and what that
 * failure would cost an analyst reading the result; each was watched go red by the deletion its
 * comment names.
 *
 * <p>The oracle for HIT is {@link IndexPruner#prune}: on a fully covered table the two must agree
 * exactly, and on a partially covered one they must NOT — the second assertion is what stops the
 * first from passing for the wrong reason.
 */
class HuntPassTest {
  @TempDir Path tmp;

  private static final String COLUMN = LocalTableFixture.COLUMN;
  private static final String NEEDLE = "needle";

  /**
   * A fresh reader per hunt. {@link TermIndex} caches a field's coverage for 30 s per instance,
   * and a test that appends a file after the build must read the coverage the build wrote, not a
   * memo of it.
   */
  private static TermIndex reader(BuildConfig config) {
    return new TermIndex(config.format(), new Metrics());
  }

  private static List<String> liveFiles(Table table) throws Exception {
    List<String> paths = new ArrayList<>();
    for (FileScanTask task : LocalTableFixture.planTasks(table)) {
      paths.add(task.file().location());
    }
    paths.sort(null);
    return paths;
  }

  /** What the pruner keeps for a MATCH on {@code token} — the existing whole-table reader. */
  private static List<String> prunerKeeps(Table table, BuildConfig config, TermIndex reader,
      String token) throws Exception {
    Metrics metrics = new Metrics();
    IndexPruner pruner = new IndexPruner(reader, metrics, config.format());
    List<FileScanTask> kept = pruner.prune(table, null,
        List.of(new IndexPruner.ContainsHint(COLUMN, token, IndexPruner.HintKind.MATCH)),
        LocalTableFixture.planTasks(table));
    return kept.stream().map(t -> t.file().location()).sorted().toList();
  }

  /**
   * The top risk: a file the index never read reported as holding the term. That is fabricated
   * evidence of compromise. Red by replacing the {@code ordinal == null} branch with
   * {@code hit.add(path)} — the pruner's own reading of an uncovered file, "keep it".
   */
  @Test
  void aFileOutsideCoverageIsUnresolvedNotAHit() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha " + NEEDLE, "bravo");
    String covered = liveFiles(table).get(0);
    IndexBuilder.buildColumn(table, COLUMN, config);
    String later = LocalTableFixture.appendFile(table, "f2.parquet", "charlie " + NEEDLE);
    table.refresh();

    HuntPass.Partition p = new HuntPass(reader(config)).hunt(table, COLUMN, NEEDLE);

    assertEquals(List.of(covered), p.hit(),
        "only the file the index actually read may be a hit");
    assertEquals(List.of(later), p.unresolved(),
        "a file added after the build is outside coverage: not examined, so not a verdict");
    assertFalse(p.hit().contains(later),
        "a file the index never read was reported as holding the term — fabricated evidence");
    assertTrue(p.miss().isEmpty());
  }

  /**
   * The false negative: "clean" declared over a file that was never examined and that holds the
   * term. An analyst closes the investigation. Red by replacing the {@code ordinal == null} branch
   * with {@code miss.add(path)} — reading the dictionary's silence about an uncovered file as
   * absence.
   */
  @Test
  void aFileOutsideCoverageIsUnresolvedNotAMiss() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha", "bravo");
    String covered = liveFiles(table).get(0);
    IndexBuilder.buildColumn(table, COLUMN, config);
    // The term is ONLY in the file the index did not read.
    String later = LocalTableFixture.appendFile(table, "f2.parquet", "charlie " + NEEDLE);
    table.refresh();

    HuntPass.Partition p = new HuntPass(reader(config)).hunt(table, COLUMN, NEEDLE);

    assertTrue(p.hit().isEmpty(), "nothing the index read holds the term");
    assertEquals(List.of(covered), p.miss(),
        "the covered file is a genuine miss: the dictionary proves the term is not in it");
    assertEquals(List.of(later), p.unresolved());
    assertFalse(p.miss().contains(later),
        "declared clean a file that was never examined and that holds the term");
    assertNotEquals(p.indexSnapshotId(), p.snapshotId(),
        "the gap between the indexed snapshot and the table's must be visible, not hidden");
  }

  /**
   * The oracle. On a fully covered table HIT is exactly what the pruner keeps — two readers of one
   * bitmap. Red by deleting the ordinal lookup so every covered file is a hit.
   */
  @Test
  void hitEqualsWhatThePrunerKeepsWhenCoverageIsComplete() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha " + NEEDLE, "bravo");
    LocalTableFixture.appendFile(table, "f2.parquet", "charlie", "delta");
    table.refresh();
    IndexBuilder.buildColumn(table, COLUMN, config); // covers both files
    TermIndex reader = reader(config);

    HuntPass.Partition p = new HuntPass(reader).hunt(table, COLUMN, NEEDLE);
    List<String> kept = prunerKeeps(table, config, reader, NEEDLE);

    assertEquals(kept, p.hit(), "with complete coverage, hit must equal what the pruner keeps");
    assertEquals(1, p.hit().size());
    assertEquals(1, p.miss().size());
    assertTrue(p.unresolved().isEmpty(), "every live file is covered, so nothing is unresolved");
    assertEquals(p.snapshotId(), p.indexSnapshotId());
    List<String> all = new ArrayList<>(p.hit());
    all.addAll(p.miss());
    all.addAll(p.unresolved());
    all.sort(null);
    assertEquals(liveFiles(table), all, "every live file lands in exactly one list");
  }

  /**
   * The inverse of the oracle, so the oracle cannot pass for the wrong reason. With partial
   * coverage the pruner KEEPS the uncovered file (advisory keep, correct for a scan) and the hunt
   * must not call that a hit. Red by deleting the {@code unresolved} branch: the two then agree
   * and this test fails.
   */
  @Test
  void hitDivergesFromThePrunerWhenCoverageIsPartial() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha", "bravo");
    IndexBuilder.buildColumn(table, COLUMN, config);
    String later = LocalTableFixture.appendFile(table, "f2.parquet", "charlie " + NEEDLE);
    table.refresh();
    TermIndex reader = reader(config);

    List<String> kept = prunerKeeps(table, config, reader, NEEDLE);
    assertEquals(List.of(later), kept,
        "precondition: the pruner keeps the uncovered file, because it cannot rule it out");

    HuntPass.Partition p = new HuntPass(reader).hunt(table, COLUMN, NEEDLE);
    assertTrue(p.hit().isEmpty(),
        "the hunt reported the pruner's advisory keep as a hit");
    assertEquals(List.of(later), p.unresolved());
    assertNotEquals(kept, p.hit(), "hit and kept must diverge exactly where coverage ends");
  }

  /**
   * A token the index's analyzer never admits is absent from the dictionary, and absence reads as
   * a miss for every covered file: a clean bill of health for a term that was never looked for.
   * The ASCII v3 contract caps token length at 256 (the fixture's default), so a 300-character run
   * is one term to {@code queryTerms} and inadmissible to {@code isIndexable}. Red by deleting
   * the {@code isIndexable} check: the hunt then returns a partition with every file a miss.
   */
  @Test
  void aTokenTheIndexAnalyzerWillNotAdmitRefuses() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    String tooLong = "a".repeat(300);
    Table table = LocalTableFixture.createTable(tmp, "alpha " + tooLong);
    IndexBuilder.buildColumn(table, COLUMN, config);

    HuntPass.Refused refused = assertThrows(HuntPass.Refused.class,
        () -> new HuntPass(reader(config)).hunt(table, COLUMN, tooLong));
    assertTrue(refused.getMessage().contains("not admitted"), refused.getMessage());
    assertTrue(refused.getMessage().contains("kahshe-ascii-v3"),
        "the refusal names the analyzer whose rule decided it: " + refused.getMessage());
  }

  /**
   * Measured on the lab proxy: a {@code match} hint on a column that does not exist answers 200
   * with every file kept. For a hunt that is "the IOC is in every file", for a typo. Red by
   * deleting the {@code field == null} check: the failure becomes a NullPointerException, which is
   * not a refusal.
   */
  @Test
  void anUnknownColumnRefusesRatherThanKeepingEveryFile() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha " + NEEDLE);
    IndexBuilder.buildColumn(table, COLUMN, config);

    HuntPass.Refused refused = assertThrows(HuntPass.Refused.class,
        () -> new HuntPass(reader(config)).hunt(table, "nosuchcol", NEEDLE));
    assertTrue(refused.getMessage().contains("nosuchcol"), refused.getMessage());
  }

  /**
   * {@code entriesFor} throws on a leaf it cannot read, by contract; the pruner catches that and
   * keeps every file. A hunt that did the same — or that swallowed the exception into an empty
   * map — would report every covered file as a miss: zero hits, from a leaf nobody read. Red by
   * replacing the catch with {@code entries = Map.of()}.
   */
  @Test
  void anUnreadableLeafRefusesRatherThanReportingZeroHits() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha " + NEEDLE);
    IndexBuilder.buildColumn(table, COLUMN, config);
    TermIndex reader = reader(config);
    // Load first, so the coverage is memoised; then take the leaves away underneath it.
    TermIndex.Loaded loaded =
        reader.forField(table, table.schema().findField(COLUMN).fieldId());
    int removed = 0;
    for (String leaf : loaded.aggregateLeaves()) {
      if (leaf == null || leaf.isEmpty()) {
        continue;
      }
      Path path = leaf.startsWith("file:") ? Path.of(URI.create(leaf)) : Path.of(leaf);
      Files.delete(path);
      removed++;
    }
    assertTrue(removed > 0, "the build wrote no aggregate leaf to remove; the test proves nothing");

    HuntPass.Refused refused = assertThrows(HuntPass.Refused.class,
        () -> new HuntPass(reader).hunt(table, COLUMN, NEEDLE));
    assertTrue(refused.getMessage().contains("unreadable"), refused.getMessage());
  }

  /**
   * Blooms and grams can say a substring MAY be in a file; only the dictionary says a term IS in
   * exactly these files and no other covered one. A hunt over an index with the term tier off has
   * no exact answer to give. Red by deleting the {@code index == null} check.
   */
  @Test
  void aColumnWithNoTermIndexRefuses() throws Exception {
    BuildConfig config = LocalTableFixture.configWithoutTermIndex();
    Table table = LocalTableFixture.createTable(tmp, "alpha " + NEEDLE);
    IndexBuilder.buildColumn(table, COLUMN, config);

    HuntPass.Refused refused = assertThrows(HuntPass.Refused.class,
        () -> new HuntPass(reader(config)).hunt(table, COLUMN, NEEDLE));
    assertTrue(refused.getMessage().contains("no term index"), refused.getMessage());
  }
}
