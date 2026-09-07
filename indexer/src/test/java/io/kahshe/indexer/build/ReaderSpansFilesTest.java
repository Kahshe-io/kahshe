package io.kahshe.indexer.build;

import io.kahshe.format.type.term.TermIndex;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.Metrics;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;

/**
 * A reader thread that handles more than one data file must attribute every file's terms to that
 * file, including the last one it reads.
 *
 * <p>Reaching that case takes a corpus deliberately larger than a test usually needs. The other
 * corpora here are about six data files, {@code IndexBuilder} defaults to eight reader threads, and
 * the wave loop takes {@code min(threads, remaining)} — so such a build is a single wave of one
 * file per thread and a reader never sees a second file. That is not a small gap: a reader owns a
 * fixed byte arena which is <b>reused across files</b> rather than allocated per file, which is the
 * whole reason its records carry an ordinal. The behaviour that reuse introduces is, by
 * construction, the behaviour a small corpus cannot exercise.
 *
 * <p>Two distinct failures live here and both are false negatives — a term the index does not
 * record is a data file wrongly pruned:
 *
 * <ul>
 *   <li><b>The tail.</b> A reader flushes when its buffer fills. Whatever is still held when it
 *       runs out of work has to be flushed too, and the only lifecycle hook near that point is
 *       {@code pool.shutdownNow()}, which runs <i>before</i> the merge. Lose the tail and the last
 *       file a thread touched contributes nothing.
 *   <li><b>The attribution.</b> With one buffer spanning several files, the ordinal is no longer
 *       implied by which reader is running. A term must be recorded against the file it came from,
 *       not the reader's first or current file.
 * </ul>
 *
 * <p>The corpus is built so those two are distinguishable: every file carries a marker term unique
 * to it, so "which file holds this term" has exactly one right answer and a wrong ordinal is not
 * hidden by a term that several files share. It also carries a term every file shares, so an
 * attribution bug that collapses ordinals shows up as a file count rather than as a missing term.
 */
class ReaderSpansFilesTest {
  @TempDir Path tmp;

  /** Comfortably more files than readers, so a wave cannot cover the corpus. */
  private static final int FILES = 12;

  private static final int READERS = 2;

  @Test
  void everyFileAReaderHandlesIsAttributedToItself() throws Exception {
    Path dir = tmp.resolve("spans");
    Files.createDirectories(dir);

    // file i holds a marker unique to it, plus a term every file shares
    Table table = LocalTableFixture.createTable(dir, rows(0));
    for (int i = 1; i < FILES; i++) {
      LocalTableFixture.appendFile(table, "f" + (i + 1) + ".parquet", rows(i));
    }

    BuildConfig config =
        LocalTableFixture.config(
            true, true, 16L * 1024 * 1024 * 1024,
            tmp.resolve("spans-runs").toString(), READERS, 1L << 20);

    assertNotNull(
        IndexBuilder.buildColumn(
            table, LocalTableFixture.COLUMN, config, IndexBuildListener.NONE,
            "p", "ns", "t", new Metrics()));

    // The precondition, asserted rather than assumed. If a later change raises the default reader
    // count or shrinks this corpus, the test silently stops testing what it is named for.
    assertTrue(
        FILES > config.indexThreads() * 2,
        "corpus must be large enough that a reader handles several files: "
            + FILES + " files against " + config.indexThreads() + " readers");

    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    TermIndex index = new TermIndex(config.format(), new Metrics());
    TermIndex.Loaded loaded = index.forField(table, fieldId);
    assertNotNull(loaded, "the build published no term index");
    assertEquals(FILES, loaded.files().size(), "not every data file was covered");

    java.util.Set<String> markers = new java.util.TreeSet<>();
    for (int i = 0; i < FILES; i++) {
      markers.add(marker(i));
    }
    var found = index.entriesFor(table, loaded, markers);

    // every marker is present -- this is what a lost tail flush breaks
    assertEquals(
        markers, new java.util.TreeSet<>(found.keySet()),
        "a file's marker term is missing from the index, so that file's terms were never recorded");

    // and every marker names exactly ONE file -- this is what a wrong ordinal breaks
    for (String m : markers) {
      TermIndex.Entry entry = found.get(m);
      assertEquals(
          1, entry.ordinals().getCardinality(),
          "marker [" + m + "] names " + entry.ordinals().getCardinality()
              + " files; it exists in exactly one");
    }

    // the markers must name twelve DISTINCT files, not one file twelve times: an attribution bug
    // that pins every term to a reader's first ordinal still gives each marker cardinality 1
    java.util.Set<Integer> named = new java.util.TreeSet<>();
    for (String m : markers) {
      found.get(m).ordinals().forEach((org.roaringbitmap.IntConsumer) named::add);
    }
    assertEquals(
        FILES, named.size(),
        "the " + FILES + " markers resolved to only " + named.size()
            + " distinct files, so terms were attributed to the wrong data file");

    // and the shared term must name every file, which no per-marker assertion above can catch
    var shared = index.entriesFor(table, loaded, List.of("everywhere"));
    assertEquals(
        FILES, shared.get("everywhere").ordinals().getCardinality(),
        "the term every file contains does not name every file");
  }

  /** File {@code i}: a marker unique to it, and a term shared by all. */
  private static String[] rows(int i) {
    return new String[] {marker(i) + " everywhere", "everywhere " + marker(i) + " filler"};
  }

  /**
   * Markers sort into the same term range (they share a first character), so a build that routed by
   * range cannot accidentally pass this by splitting them across leaves.
   */
  private static String marker(int i) {
    return "zmarker" + i;
  }
}
