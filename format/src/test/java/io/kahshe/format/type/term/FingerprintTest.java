package io.kahshe.format.type.term;

import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.build.IndexBuilder;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.format.IndexPaths;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.format.type.term.TermIndexWriter;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.nio.file.Path;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The term index's revalidation token moves when the leaves are rewritten, even at the SAME
 * snapshot, without reading a single leaf's length: the per-publish nonce in every range leaf's
 * name is what carries the generation.
 *
 * <p>A full rebuild at an unchanged snapshot is the case a snapshot-only token would miss.
 * Verified red with {@code fingerprintOf} reduced to the snapshot id: the two tokens compare equal.
 */
class FingerprintTest {
  @TempDir Path tmp;

  @Test
  void aRebuildAtTheSameSnapshotMovesTheTokenThroughTheLeafNames() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha bravo");
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    String first = TermIndex.fingerprint(table, fieldId, config.format());
    assertEquals(first, TermIndex.fingerprint(table, fieldId, config.format()), "stable while nothing moves");

    // Force a FULL rebuild at the same snapshot: with the term metadata gone the coverage is
    // empty, so the next build re-reads every file and publishes leaves under fresh nonces.
    FileIO io = IndexPaths.io(table, config.format());
    io.deleteFile(
        TermIndexWriter.dir(IndexPaths.root(table, config.format().indexRoot()), fieldId)
            + "/index-metadata.json");
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    String second = TermIndex.fingerprint(table, fieldId, config.format());

    assertNotEquals("absent", second);
    assertNotEquals(
        first, second,
        "same snapshot, rewritten leaves: the token must move, or a reader renews stale data for "
            + "the life of the process");
  }
}
