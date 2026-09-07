package io.kahshe.format.type.bloom;

import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.build.IndexBuilder;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.format.type.bloom.IndexMeta;
import io.kahshe.format.IndexPaths;
import io.kahshe.format.IndexPruner;
import io.kahshe.format.type.bloom.IndexStore;
import io.kahshe.format.type.term.TermIndex;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.Metrics;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The bloom leaf list must not grow without bound.
 *
 * <p>{@code writeBloomLeaf} carries the prior generation's list forward and appends, and
 * {@code IndexStore.load} opens EVERY entry on a cold read — so without a bound an actively
 * maintained table gains one leaf per incremental build forever and pays a round trip per leaf on
 * every content change. No GC pass can help, because every one of those leaves is REFERENCED; the
 * only fix is for the build to rewrite them.
 *
 * <p>The property that matters is not "compaction happened" but "compaction lost nothing": a bloom
 * that goes missing means that file is no longer probed, which is safe (it is kept) but silently
 * stops pruning it.
 */
class BloomLeafCompactionTest {
  @TempDir Path tmp;

  private static List<FileScanTask> plan(Table table) throws Exception {
    return LocalTableFixture.planTasks(table);
  }

  /**
   * Verified by breaking it: raising {@code BLOOM_MAX_LEAVES} above the build count leaves the
   * list growing one per build, which fails the bound assertion below; dropping the
   * {@code merged.putAll(blooms)} so compaction forgets the newest file fails the pruning
   * assertion instead. The bound is stated against the build count rather than against
   * {@code BLOOM_MAX_LEAVES} precisely so that raising the constant cannot satisfy it.
   */
  @Test
  void theLeafListIsBoundedAndCompactionKeepsEveryFileProbeable() throws Exception {
    // TERM TIER OFF, deliberately. This exercises the bloom leaf list, and the term tier writes
    // 36 aggregate range leaves plus spill runs per build -- which under Hadoop's local
    // FileSystem means a forked chmod per file operation, so ten builds of it cost minutes and say
    // nothing about blooms.
    BuildConfig config = LocalTableFixture.configWithoutTermIndex();
    Table table = LocalTableFixture.createTable(tmp, "alpha alpha");
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    // one incremental build per appended file, past the compaction threshold
    List<String> paths = new ArrayList<>();
    int builds = BloomLeaf.BLOOM_MAX_LEAVES + 2;
    for (int i = 2; i <= builds; i++) {
      paths.add(LocalTableFixture.appendFile(table, "f" + i + ".parquet", "marker" + i + " shared"));
      table.refresh();
      assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));
    }

    Metrics metrics = new Metrics();
    IndexStore store = new IndexStore(config.format(), metrics);
    IndexStore.LoadedIndex loaded = store.forColumn(table, LocalTableFixture.COLUMN);
    assertNotNull(loaded);

    // EXACTLY two, and the arithmetic is worth stating because it pins the mechanism rather than
    // a vague bound. Builds 1-8 append (the list reaches BLOOM_MAX_LEAVES). Build 9 would make
    // nine, so it compacts to one. Build 10 appends to that, giving two. Without compaction this
    // is 10 -- one per build, forever.
    //
    // Stated against a literal rather than against BLOOM_MAX_LEAVES on purpose: comparing the
    // count to the same constant you would raise to disable compaction makes the assertion
    // vacuously true instead of false. A test that varies a knob must not measure itself against
    // that knob.
    assertEquals(
        2,
        metrics.indexBloomLeaves.getAsLong(),
        "the bloom leaf list is not being compacted; it grows one entry per build and every one "
            + "of them is opened on a cold read");

    // NOTHING WAS LOST. Every file the table has must still carry a bloom, or it silently stops
    // being probed -- which is safe, and invisible, and exactly what compaction could break.
    for (FileScanTask task : plan(table)) {
      assertNotNull(
          loaded.blooms().get(task.file().location()),
          "compaction dropped the bloom for " + task.file().location());
    }
    assertEquals(builds, loaded.blooms().size(), "compaction changed the file count");

    // and the blooms still discriminate: each marker keeps exactly its own file
    IndexPruner pruner = new IndexPruner(new TermIndex(config.format(), metrics), metrics, config.format());
    List<IndexPruner.ContainsHint> hint = List.of(
        new IndexPruner.ContainsHint(
            LocalTableFixture.COLUMN, "marker" + builds, IndexPruner.HintKind.CONTAINS));
    List<FileScanTask> kept = pruner.prune(table, null, hint, plan(table));
    assertEquals(1, kept.size(), "a marker unique to the newest file no longer prunes to it");
    assertEquals(paths.get(paths.size() - 1), kept.get(0).file().location());
  }

  /**
   * Compaction must leave the superseded leaves unreferenced rather than deleted.
   *
   * <p>Deleting them inline would put object removal on the build's critical path and race every
   * reader still holding the prior metadata. Orphaning them is the safe move: the grace-period GC
   * reclaims them later, and until then they cost only storage.
   */
  @Test
  void compactionOrphansThePriorLeavesInsteadOfDeletingThem() throws Exception {
    BuildConfig config = LocalTableFixture.configWithoutTermIndex();
    Table table = LocalTableFixture.createTable(tmp, "alpha alpha");
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));
    for (int i = 2; i <= BloomLeaf.BLOOM_MAX_LEAVES + 2; i++) {
      LocalTableFixture.appendFile(table, "f" + i + ".parquet", "marker" + i);
      table.refresh();
      assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));
    }

    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    Path dir = Path.of(IndexMeta.dir(IndexPaths.root(table, config.format().indexRoot()), fieldId));
    long onDisk;
    try (var files = java.nio.file.Files.list(dir)) {
      onDisk = files.filter(f -> f.getFileName().toString().endsWith(".parquet")).count();
    }
    IndexStore.LoadedIndex loaded =
        new IndexStore(config.format(), new Metrics()).forColumn(table, LocalTableFixture.COLUMN);
    assertNotNull(loaded);
    int referenced = IndexStore.leaves(loaded.meta()).size();

    assertTrue(referenced < onDisk,
        "expected superseded leaves to remain on disk as orphans, but every leaf present ("
            + onDisk + ") is still referenced (" + referenced + ")");
  }
}
