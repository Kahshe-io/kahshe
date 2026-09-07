package io.kahshe.format.type.bloom;

import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.build.IndexBuilder;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.format.type.bloom.IndexMeta;
import io.kahshe.format.IndexPaths;
import io.kahshe.format.type.bloom.IndexStore;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.format.type.term.TermIndexWriter;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Rebuilding the same snapshot in place must be visible to a reader that already loaded the index.
 *
 * <p>{@code IndexStore} decides whether to reload from a fingerprint of (snapshot id, leaf files),
 * and refreshes nothing when it has not moved. The index uuid cannot help: it is deliberately
 * carried forward from the prior generation because it identifies the index, not the generation.
 *
 * <p>So the leaf NAMES are the only thing that can carry the signal, and a name of the form
 * {@code leaf-<snapshotId>.parquet} cannot: same snapshot, same name, same fingerprint, and a
 * reader that has already loaded the old index keeps serving it forever. No exception, no metric,
 * no way to notice from outside except that pruning quietly stops improving.
 *
 * <p>This is the shape of that failure rather than a unit test of the naming function: build,
 * rebuild, and assert the fingerprint inputs actually moved.
 */
class IndexFreshnessOnRebuildTest {
  @TempDir Path tmp;

  private static int fieldId(Table table) {
    return table.schema().findField(LocalTableFixture.COLUMN).fieldId();
  }

  @Test
  void twoLeafWritesForOneSnapshotDoNotShareAName() {
    String a = IndexMeta.leafPath("root", 3, 77L, "aaaa1111");
    String b = IndexMeta.leafPath("root", 3, 77L, "bbbb2222");
    assertNotEquals(a, b, "two writes of one snapshot produced the same leaf object");
    assertTrue(a.contains("leaf-77-"), a);
    assertTrue(a.endsWith(".parquet"), a);
  }

  @Test
  void rebuildingTheSameSnapshotChangesTheLeafSetAReaderFingerprints() throws Exception {
    io.kahshe.indexer.BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha beta", "beta gamma");
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    long snapshot = table.currentSnapshot().snapshotId();
    List<String> first = leavesOf(table, config);
    assertTrue(!first.isEmpty(), "the first build wrote no leaf");

    // A build that finds a current index correctly does nothing, so force the case that matters:
    // drop the metadata and rebuild. That is a repair, a format bump, or the loser of a race — the
    // situations where the same snapshot genuinely gets indexed twice, and the ones where an
    // unchanged leaf name leaves every reader that has already loaded the index serving it forever.
    IndexPaths.io(table, config.format())
        .deleteFile(
            IndexMeta.metaPath(IndexPaths.root(table, config.format().indexRoot()), fieldId(table)));
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    table.refresh();
    assertEquals(snapshot, table.currentSnapshot().snapshotId(), "the table moved; wrong scenario");
    List<String> second = leavesOf(table, config);

    assertNotEquals(first, second,
        "a rebuild of the same snapshot left the leaf set unchanged, so a reader holding the old "
            + "index would never reload it");
  }

  /**
   * The term tier: an in-place rewrite of a NONCE-NAMED leaf is deliberately NOT revalidated.
   *
   * <p>Folding every leaf's byte length into the token costs 36 object-store HEADs per (table,
   * field) per TTL expiry, on the request thread. The writer never rewrites a nonce-named path
   * ({@code twoLeafWritesForOneSnapshotDoNotShareAName} pins that, and
   * {@code rebuildingTheSameSnapshotChangesTheLeafSetAReaderFingerprints} plus
   * {@code FingerprintTest} pin that a rebuild at the same snapshot moves the token through the
   * names), so the fold defends against a write that cannot happen. This test pins the cost
   * decision: a hand-forged in-place rewrite of a nonce-named leaf leaves the token unchanged, and
   * re-adding the fold turns it red. Known gap: the legacy single-leaf layout (no nonce in the
   * name) still folds the length, and no test forges that layout.
   *
   * <p>What the defence is for, since dropping it is only safe under the argument above. The bloom
   * tier closes this with a per-write nonce (the test above) and the gram tier by folding its
   * leaf's byte length into the revalidation token. A term tier with neither -- a token of
   * (snapshot id, aggregate path), over an aggregate leaf named for the snapshot and written with
   * overwrite() -- produces a byte-identical token when one snapshot's index is rewritten in place.
   * A reader that has already cached the old payload then recomputes the same token at every TTL
   * expiry and renews stale data for the life of the process: no error, no metric.
   *
   * <p>This asserts the TOKEN rather than an end-to-end reload, deliberately. Reaching the failure
   * through the build requires differing content at a FIXED snapshot, which needs a first build
   * that covered fewer files than the second -- an interrupted or partially-failed build. Every
   * end-to-end route moves the snapshot too, which moves the token by itself and so would pass with
   * the defect present. A test that cannot fail is worse than no test, so this one goes at the
   * property directly.
   */
  @Test
  void anInPlaceRewriteOfANonceNamedLeafIsNotRevalidatedByDesign() throws Exception {
    io.kahshe.indexer.BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha beta", "beta gamma");
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);

    String before = TermIndex.fingerprint(table, fieldId(table), config.format());
    assertNotEquals("absent", before);
    assertNotEquals("unreadable", before);

    // rewrite the aggregate leaf in place, same path, same snapshot, different bytes -- which no
    // build does: every publish takes a fresh nonce, so this is a forged write
    var io = IndexPaths.io(table, config.format());
    var metaFile =
        io.newInputFile(
            TermIndexWriter.dir(IndexPaths.root(table, config.format().indexRoot()), fieldId(table))
                + "/index-metadata.json");
    String aggregate;
    try (var in = metaFile.newStream()) {
      // the first range leaf that actually exists; empty ranges have no object to rewrite
      aggregate =
          TermIndexWriter.aggregateLeaves(
                  TermIndexWriter.snapshotNode(
                      new com.fasterxml.jackson.databind.ObjectMapper().readTree(in)))
              .stream()
              .filter(leaf -> !leaf.isEmpty())
              .findFirst()
              .orElseThrow(() -> new AssertionError("the build wrote no aggregate range leaf"));
    }
    Path leaf = Path.of(aggregate);
    byte[] original = java.nio.file.Files.readAllBytes(leaf);
    java.nio.file.Files.write(leaf, java.util.Arrays.copyOf(original, original.length + 4096));
    java.nio.file.Files.deleteIfExists(leaf.resolveSibling("." + leaf.getFileName() + ".crc"));

    assertEquals(
        before,
        TermIndex.fingerprint(table, fieldId(table), config.format()),
        "a nonce-named leaf is identified by its name, not its length: re-adding the length fold "
            + "buys 36 HEADs per expiry against a rewrite the writer never performs");
  }

  private List<String> leavesOf(Table table, io.kahshe.indexer.BuildConfig config) throws Exception {
    var io = IndexPaths.io(table, config.format());
    var metaFile =
        io.newInputFile(
            IndexMeta.metaPath(IndexPaths.root(table, config.format().indexRoot()), fieldId(table)));
    try (var in = metaFile.newStream()) {
      return IndexStore.leaves(
          IndexMeta.parse(new com.fasterxml.jackson.databind.ObjectMapper().readTree(in)));
    }
  }
}
