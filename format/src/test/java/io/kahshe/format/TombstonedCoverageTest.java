package io.kahshe.format;

import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.build.IndexBuilder;
import io.kahshe.indexer.LocalTableFixture;
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
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A data file leaving the table must not renumber the files that stayed.
 *
 * <p>Were an ordinal a file's POSITION in the coverage list, removing one would shift every later
 * ordinal and silently repoint every bitmap in the gram and term tiers at the wrong file. The
 * defence available to a positional scheme — refusing to go incremental whenever a covered file has
 * disappeared — is correct and ruinously expensive: rolling retention removes files daily, and
 * every removal then costs a full rebuild of the whole table.
 *
 * <p>So ordinals are allocated, and a departed file is tombstoned in place. These tests pin both
 * halves of that: the build stays incremental across a removal, AND the files that survived still
 * resolve to the ordinals their bitmaps were written with.
 */
class TombstonedCoverageTest {
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

  /** Which files survive a MATCH on {@code token} in a scan of {@code snapshotId}. */
  private static List<String> keptForSnapshot(
      Table table, BuildConfig config, String token, long snapshotId) throws Exception {
    Metrics metrics = new Metrics();
    IndexPruner pruner = new IndexPruner(new TermIndex(config.format(), metrics), metrics, config.format());
    List<FileScanTask> kept =
        pruner.prune(
            table,
            null,
            List.of(
                new IndexPruner.ContainsHint(
                    LocalTableFixture.COLUMN, token, IndexPruner.HintKind.MATCH)),
            LocalTableFixture.planTasks(table, snapshotId));
    return kept.stream().map(t -> t.file().location()).sorted().toList();
  }

  /** Which live files survive a MATCH on {@code token}. */
  private static List<String> keptFor(Table table, BuildConfig config, String token)
      throws Exception {
    Metrics metrics = new Metrics();
    IndexPruner pruner = new IndexPruner(new TermIndex(config.format(), metrics), metrics, config.format());
    List<FileScanTask> kept =
        pruner.prune(
            table,
            null,
            List.of(
                new IndexPruner.ContainsHint(
                    LocalTableFixture.COLUMN, token, IndexPruner.HintKind.MATCH)),
            LocalTableFixture.planTasks(table));
    return kept.stream().map(t -> t.file().location()).toList();
  }

  /**
   * The whole point of the change, in one test.
   *
   * <p>Three files are indexed, the MIDDLE one is dropped, and a fourth is appended. The token that
   * matters is {@code charlie}, which lives only in the file AFTER the one removed — under
   * positional ordinals its number would slide from 2 to 1 and its bitmap would then name the file
   * that used to be at 1.
   *
   * <p>Verified both ways. Restoring the old gate ({@code && currentSet.containsAll(covered)}) makes
   * the incremental assertion fail: the build re-reads all three surviving files instead of one.
   * Keeping the gate off while numbering positionally instead of by allocation makes the pruning
   * assertion fail, because {@code charlie} then resolves to the wrong file.
   */
  @Test
  void aRemovedFileDoesNotRenumberTheFilesThatStayed() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha alpha");
    String f2 = LocalTableFixture.appendFile(table, "f2.parquet", "bravo bravo");
    String f3 = LocalTableFixture.appendFile(table, "f3.parquet", "charlie charlie");
    table.refresh();

    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));
    Map<String, Integer> before =
        Coverage.ordinalOfLive(coverageOf(table, config));
    assertEquals(3, before.size(), "precondition: three files covered");
    assertEquals(List.of(f3), keptFor(table, config, "charlie"), "precondition: charlie prunes to f3");
    int f3OrdinalBefore = before.get(f3);

    // the middle file leaves, as compaction or expiry would remove it
    table.newDelete().deleteFile(f2).commit();
    table.refresh();
    String f4 = LocalTableFixture.appendFile(table, "f4.parquet", "delta delta");
    table.refresh();

    long[] result = IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    assertNotNull(result);
    assertEquals(
        1,
        result[2],
        "a departed file forced a full rebuild: only the newly appended file should have been "
            + "read, but this build read every surviving file");

    List<Coverage.Entry> after = coverageOf(table, config);
    assertEquals(4, after.size(), "the departed file should keep its slot, not vanish: " + after);
    assertTrue(
        after.stream().anyMatch(e -> e.path().equals(f2) && !e.live()),
        "the departed file was not tombstoned: " + after);
    Map<String, Integer> ordinals = Coverage.ordinalOfLive(after);
    assertFalse(ordinals.containsKey(f2), "a dead entry must not be in the live ordinal map");
    assertEquals(
        f3OrdinalBefore,
        ordinals.get(f3),
        "a surviving file was renumbered when another file left; every bitmap naming its old "
            + "ordinal now points at a different file");

    // and the bitmaps still mean what they meant: each token resolves to exactly its own file
    assertEquals(List.of(f3), keptFor(table, config, "charlie"));
    assertEquals(List.of(f4), keptFor(table, config, "delta"));
    // a token whose only file has left prunes to nothing, rather than to some other file
    assertEquals(List.of(), keptFor(table, config, "bravo"));
  }

  /**
   * Time travel: an old snapshot still prunes, and the files it names that have since LEFT are
   * kept rather than pruned away.
   *
   * <p>This is the path where the tasks handed to the pruner contain files the index has
   * tombstoned — exactly the inputs the live-only ordinal map exists to handle, and inputs a plan
   * of the current snapshot never produces.
   *
   * <p>Why it is sound rather than lucky: the index is keyed by file PATH, and Iceberg data files
   * are immutable, so "this term is in this file" is true for every snapshot containing it. The
   * snapshot stamp on the index records when it was BUILT, not what it is valid for. A departed
   * file is absent from {@code ordinalOfLive}, and an absent path is kept by every pruner — so an
   * old snapshot prunes its survivors exactly and carries its departed files along unpruned. That
   * degrades with age instead of breaking, and the direction is the safe one.
   *
   * <p>Verified by breaking it: making {@code Coverage.ordinalOfLive} return dead entries too
   * prunes {@code f2} out of the time-travelled scan, which loses every row in it.
   */
  @Test
  void anOldSnapshotStillPrunesAndKeepsTheFilesThatHaveSinceLeft() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha alpha");
    String f2 = LocalTableFixture.appendFile(table, "f2.parquet", "bravo bravo");
    String f3 = LocalTableFixture.appendFile(table, "f3.parquet", "charlie charlie");
    table.refresh();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));
    long oldSnapshot = table.currentSnapshot().snapshotId();

    // f2 leaves and a new file arrives, as retention plus ingest would do
    table.newDelete().deleteFile(f2).commit();
    table.refresh();
    LocalTableFixture.appendFile(table, "f4.parquet", "delta delta");
    table.refresh();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));
    assertFalse(
        Coverage.ordinalOfLive(coverageOf(table, config)).containsKey(f2),
        "precondition: f2 is tombstoned, so it is absent from the live ordinal map");

    // The departed file is still IN the old snapshot's plan, and must survive pruning: the index
    // can no longer say anything about it, and "cannot say" means keep.
    assertTrue(
        LocalTableFixture.planTasks(table, oldSnapshot).stream()
            .anyMatch(t -> t.file().location().equals(f2)),
        "precondition: the old snapshot still contains the file that has since left");
    assertEquals(
        List.of(f2),
        keptForSnapshot(table, config, "bravo", oldSnapshot),
        "a token whose only file has left the table must keep that file when the old snapshot is "
            + "planned; pruning it loses every row in it");

    // And the survivors still prune exactly. f2 rides along because it is unprunable, not because
    // pruning stopped working -- f1 is dropped, which is what proves the index is still consulted.
    assertEquals(
        List.of(f2, f3).stream().sorted().toList(),
        keptForSnapshot(table, config, "charlie", oldSnapshot),
        "the old snapshot stopped pruning entirely: f1 should be dropped and only the matching "
            + "file plus the unprunable departed one kept");
  }

  /**
   * A token count cannot be exact once a file has been tombstoned, and {@code _count} keys its
   * refusal on this flag.
   *
   * <p>The aggregate's {@code total_count} is a scalar sum that still includes the departed file's
   * occurrences; the bitmap records WHICH files hold a term, not how many times each holds it, so
   * there is nothing to subtract. Pruning stays exact — it only asks which files — but counting
   * does not, and this endpoint promises exact-or-refuse.
   */
  @Test
  void anIndexThatHasLostAFileReportsItselfUncountable() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha alpha");
    String f2 = LocalTableFixture.appendFile(table, "f2.parquet", "bravo bravo");
    table.refresh();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    Metrics metrics = new Metrics();
    assertTrue(
        new TermIndex(config.format(), metrics).forField(table, fieldId(table)).countsExact(),
        "precondition: nothing has left the table yet");

    table.newDelete().deleteFile(f2).commit();
    table.refresh();
    LocalTableFixture.appendFile(table, "f3.parquet", "charlie charlie");
    table.refresh();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    assertFalse(
        new TermIndex(config.format(), new Metrics()).forField(table, fieldId(table)).countsExact(),
        "a file left the table but the index still claims exact counts, so _count would return an "
            + "upper bound labelled exact");
  }

  /**
   * A file that comes back is revived at its original ordinal rather than re-read.
   *
   * <p>Iceberg data files are immutable, so a path that returns — a rollback, a re-registered table
   * — has the bytes it had when it was indexed, and the bitmaps naming its old ordinal still
   * describe it exactly. Allocating it a second ordinal would both cost a re-read and put two
   * entries in the map for one path.
   */
  @Test
  void aReturningFileIsRevivedAtItsOriginalOrdinal() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha alpha");
    String f2 = LocalTableFixture.appendFile(table, "f2.parquet", "bravo bravo");
    table.refresh();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));
    int f2Ordinal = Coverage.ordinalOfLive(coverageOf(table, config)).get(f2);

    long snapshotWithF2 = table.currentSnapshot().snapshotId();
    table.newDelete().deleteFile(f2).commit();
    table.refresh();
    LocalTableFixture.appendFile(table, "f3.parquet", "charlie charlie");
    table.refresh();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));
    assertTrue(
        coverageOf(table, config).stream().anyMatch(e -> e.path().equals(f2) && !e.live()),
        "precondition: the file is tombstoned before it returns");

    // roll the table back to a snapshot that still holds f2, then rebuild
    table.manageSnapshots().rollbackTo(snapshotWithF2).commit();
    table.refresh();
    long[] result = IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    assertNotNull(result);
    // dataBytes, not the file count: with no NEW paths this takes the restamp path, whose third
    // slot reports live coverage rather than files read. Bytes read is zero in both paths and is
    // the honest "nothing was opened" signal.
    assertEquals(0, result[3], "a revived file was re-read; its ordinal's bitmaps already describe it");

    Map<String, Integer> ordinals = Coverage.ordinalOfLive(coverageOf(table, config));
    assertEquals(f2Ordinal, ordinals.get(f2), "the revived file did not get its original ordinal back");
    assertEquals(List.of(f2), keptFor(table, config, "bravo"), "the revived file no longer matches");
  }

  /**
   * A legacy file list — a bare array of paths, with no ordinal or liveness per entry — reads as
   * "every file live, ordinal = position", which is exactly what it meant. Without this an index
   * in that shape loads with an empty ordinal map and silently stops pruning.
   */
  @Test
  void aLegacyPositionalFileListStillMeansWhatItMeant() {
    com.fasterxml.jackson.databind.node.ArrayNode legacy =
        new ObjectMapper().createArrayNode().add("s3://b/a.parquet").add("s3://b/b.parquet");
    List<Coverage.Entry> entries = Coverage.parse(legacy);

    assertEquals(2, entries.size());
    assertEquals(Map.of("s3://b/a.parquet", 0, "s3://b/b.parquet", 1), Coverage.ordinalOfLive(entries));
    assertFalse(Coverage.hasTombstones(entries));
    assertEquals(2, Coverage.nextOrdinal(entries), "new files must be numbered past the legacy list");
  }

  /** A dead slot is still consumed: reusing its ordinal would hand a new file its bitmaps. */
  @Test
  void aDeadOrdinalIsNeverReissued() {
    List<Coverage.Entry> entries =
        List.of(
            new Coverage.Entry("a", 0, true),
            new Coverage.Entry("b", 1, false),
            new Coverage.Entry("c", 2, true));
    assertEquals(3, Coverage.nextOrdinal(entries));
    assertEquals(Map.of("a", 0, "c", 2), Coverage.ordinalOfLive(entries));
    assertTrue(Coverage.hasTombstones(entries));
  }
}
