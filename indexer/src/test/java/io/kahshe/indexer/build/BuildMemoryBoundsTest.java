package io.kahshe.indexer.build;

import io.kahshe.format.type.gram.GramIndex;
import io.kahshe.format.IndexPruner;
import io.kahshe.format.type.term.TermIndex;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.Metrics;
import java.nio.file.Path;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;

/**
 * Bounds that could be satisfied while the heap was exhausted.
 *
 * <p>All three share one shape: a budget that governs part of a working set, or an estimate of
 * bytes over a growable structure that is wrong in the optimistic direction. No test that runs a
 * build catches that shape, because a small corpus never reaches any budget.
 *
 * <p>So these do not try to exhaust memory. They assert the ARITHMETIC — that the estimate counts
 * what it claims to, and that the refusal fires — which is the part a small corpus can prove.
 */
class BuildMemoryBoundsTest {
  @TempDir Path tmp;


  /**
   * The gram valve's per-entry estimate must not be optimistic.
   *
   * <p>An entry costs a {@code HashMap.Node}, a {@code ByteKey}, a {@code byte[]} and a
   * {@code RoaringBitmap}, so a nominal {@code + 8} bytes is an order of magnitude low on short
   * gram keys. An under-count trips the valve late or never, which is an OOM; an over-count trips
   * it early, which drops the gram layer and leaves the table serving from blooms.
   */
  @Test
  void theGramEntryEstimateExceedsAPlausibleFloor() {
    // a HashMap.Node alone is ~48 bytes; anything at or below that is not an estimate
    assertTrue(
        IndexBuilder.GRAM_ENTRY_OVERHEAD_BYTES >= 48,
        "per-entry gram overhead of " + IndexBuilder.GRAM_ENTRY_OVERHEAD_BYTES
            + " bytes is below the cost of the map node alone, so the valve will trip late");
  }

  /**
   * A prior grams leaf too big for the build's budget must not be read at all.
   *
   * <p>The downstream valve runs AFTER {@code readLeaf} has already deserialized the whole leaf
   * into the map, so the heap spike it would prevent has happened by the time it looks. The size is
   * recorded by the writer, so it can be refused before it is paid — which is what
   * {@code GramIndex} already does on the serving side, from the same number.
   *
   * <p>Asserted as arithmetic rather than by building a multi-gigabyte leaf: with the budget set
   * below the recorded size, the build must decline to load the prior layer and restart gram
   * coverage instead of carrying it forward.
   */
  @Test
  void anOversizedPriorGramsLeafIsNotRead() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha alpha");
    BuildConfig normal = LocalTableFixture.config();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, normal));

    LocalTableFixture.appendFile(table, "f2.parquet", "bravo bravo");
    table.refresh();

    // a gram build budget of 1 byte: any recorded prior leaf exceeds it
    BuildConfig starved =
        io.kahshe.common.Records.with(normal, java.util.Map.of("gramBuildMaxBytes", 1L));
    Metrics metrics = new Metrics();
    assertNotNull(
        IndexBuilder.buildColumn(
            table, LocalTableFixture.COLUMN, starved, IndexBuildListener.NONE, "", "", "", metrics),
        "the build must still succeed -- the gram layer degrades, it does not fail the build");
    assertTrue(
        metrics.gramTooLarge.sum() >= 1,
        "an oversized prior grams leaf was read anyway; the refusal must fire before readLeaf, "
            + "since the valve downstream only looks after the heap has already been spent");

    // and the index still serves: a token in the second file must not be pruned away
    IndexPruner pruner = new IndexPruner(new TermIndex(normal.format(), new Metrics()), new Metrics(), normal.format());
    assertEquals(
        1,
        pruner.prune(
            table, null,
            java.util.List.of(new IndexPruner.ContainsHint(
                LocalTableFixture.COLUMN, "bravo", IndexPruner.HintKind.MATCH)),
            LocalTableFixture.planTasks(table)).size());
  }

  /** The bloom map's cap must be a real number, and the build must be able to name it. */
  @Test
  void theBloomBuildCapIsSet() {
    assertTrue(IndexBuilder.BLOOM_BUILD_MAX_BYTES > 0);
    assertTrue(
        IndexBuilder.BLOOM_BUILD_MAX_BYTES <= (4L << 30),
        "a cap this large cannot fail before the heap does, which is the same as no cap");
  }
}
