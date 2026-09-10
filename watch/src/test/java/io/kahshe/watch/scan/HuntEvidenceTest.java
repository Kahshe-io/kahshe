package io.kahshe.watch.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.Metrics;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.indexer.build.IndexBuilder;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * What a hunt's evidence may honestly claim. Two labels, each decided by a fact about the table
 * or the index rather than by the hunt: whether the snapshot is proven delete-free, and whether
 * the dictionary's counts are still exact. Both are the rules the rest of the system already
 * applies; these tests pin that the hunt applies them too, against a table that really carries a
 * delete and an index that really lost a file.
 */
class HuntEvidenceTest {
  @TempDir Path tmp;

  private static final String COLUMN = LocalTableFixture.COLUMN;

  private static HuntPass hunt(BuildConfig config) {
    return new HuntPass(new TermIndex(config.format(), new Metrics()));
  }

  /**
   * The index reads raw data files and applies no delete file, so on a merge-on-read snapshot a
   * hit is a file that HELD the term, not one that still returns it — the confirmation SQL, which
   * applies the deletes, may come back empty and read as a false positive. The label is what
   * tells the analyst that is the deletes. Red by replacing the {@code deleteBearing} ternary
   * with {@code "exact"}: the snapshot below carries a position delete on the only row that
   * matches, and the hunt would still call the hit exact.
   */
  @Test
  void confidenceIsAdvisoryOnASnapshotThatCarriesDeletes() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha needle", "bravo");
    IndexBuilder.buildColumn(table, COLUMN, config);
    HuntPass.Partition before = hunt(config).hunt(table, COLUMN, "needle");
    assertEquals("exact", before.confidence(),
        "precondition: an append-only snapshot proves total-delete-files = 0");
    assertEquals("0", table.currentSnapshot().summary().get("total-delete-files"));

    String dataFile = before.hit().get(0);
    LocalTableFixture.appendPositionDelete(table, dataFile, 0); // the row holding the needle
    table.refresh();
    assertEquals("1", table.currentSnapshot().summary().get("total-delete-files"),
        "precondition: the snapshot now carries a delete file");

    HuntPass.Partition after = hunt(config).hunt(table, COLUMN, "needle");
    assertEquals(List.of(dataFile), after.hit(),
        "the file still holds the term in its raw rows, so the index still says hit");
    assertEquals("advisory", after.confidence(),
        "a hit on a delete-bearing snapshot is an upper bound and must say so");
    assertEquals("advisory", after.toJson().get("confidence").asText());
  }

  /**
   * {@code total_count} sums every source ever merged into a term's row and nothing can subtract
   * a departed file's share (FORMAT.md §5.6), so after a file leaves the table the number is an
   * upper bound. {@code _count} refuses it; a hunt omits it. Red by deleting the
   * {@code countsExact} gate: the field then appears carrying the departed file's occurrences.
   */
  @Test
  void occurrencesAreReportedOnlyWhileTheCountsAreExact() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "needle needle");
    String f2 = LocalTableFixture.appendFile(table, "f2.parquet", "needle");
    table.refresh();
    IndexBuilder.buildColumn(table, COLUMN, config);

    HuntPass.Partition exact = hunt(config).hunt(table, COLUMN, "needle");
    assertTrue(exact.countsExact(), "precondition: nothing has left the table");
    assertEquals(Map.of("needle", 3L), exact.occurrences(),
        "two occurrences in one file and one in the other, across every covered file");
    assertEquals(3, exact.toJson().get("occurrences").get("needle").asLong());

    // A file leaves; an incremental rebuild that reads something new marks the counts inexact
    // (§5.6) while the total still carries f2's occurrence.
    table.newDelete().deleteFile(f2).commit();
    table.refresh();
    LocalTableFixture.appendFile(table, "f3.parquet", "charlie");
    table.refresh();
    IndexBuilder.buildColumn(table, COLUMN, config);

    HuntPass.Partition inexact = hunt(config).hunt(table, COLUMN, "needle");
    assertFalse(inexact.countsExact(), "precondition: a file left, so the counts are not exact");
    assertNull(inexact.occurrences(), "an upper bound must not be printed as a count");
    assertNull(inexact.toJson().get("occurrences"));
    assertFalse(inexact.toJson().get("counts_exact").asBoolean(), "and the summary says why");
    assertEquals(1, inexact.hit().size(), "the partition itself is unaffected: pruning never was");
  }
}
