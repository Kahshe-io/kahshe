package io.kahshe.indexer.build;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;

/**
 * The arithmetic check that stands between a configuration and an out-of-memory failure.
 *
 * <p>No test can catch such a failure by running a build: every test runs on a handful of files, a
 * scale at which no budget is exercised, so a configuration that is arithmetically impossible
 * passes everything and then dies part-way through a real one. The check therefore has to be
 * tested as arithmetic, against configurations chosen to sit either side of the line — which is
 * the one thing a small-corpus suite CAN do honestly.
 */
class BuildBudgetTest {

  private static BuildConfig knobs(int threads, long bufferBytes, long cacheBytes, long gramCap) {
    return io.kahshe.common.Records.with(
        LocalTableFixture.config(),
        java.util.Map.of(
            "indexThreads", threads,
            "termBufferBytes", bufferBytes,
            "cacheBytes", cacheBytes,
            "gramBuildMaxBytes", gramCap));
  }

  /**
   * A manifest's arithmetic that does not fit its own container.
   *
   * <p>A 2 GiB limit at {@code MaxRAMPercentage=75} is a 1.5 GiB heap. These knobs commit roughly
   * 614 MiB of caches, 8 x 64 MiB of term arenas allocated up front, and a 1 GiB gram ceiling —
   * about 2.1 GiB. With {@code ExitOnOutOfMemoryError} and the default {@code KAHSHE_MODE=both},
   * hitting that takes the serving path down with the build.
   */
  @Test
  void theOldDefaultManifestArithmeticIsRefused() {
    BuildConfig config =
        knobs(8, 64L << 20, 614L << 20, 1024L << 20); // ~2.1 GiB committed
    long heap = 1536L << 20; // 2 GiB container at MaxRAMPercentage=75

    IllegalStateException thrown =
        assertThrows(IllegalStateException.class, () -> BuildBudget.check(config, heap));
    // the message has to be actionable, not merely correct: both numbers and what to turn down
    assertTrue(thrown.getMessage().contains("KAHSHE_INDEX_THREADS"), thrown.getMessage());
    assertTrue(thrown.getMessage().contains("KAHSHE_GRAM_BUILD_MAX_BYTES"), thrown.getMessage());
    assertTrue(thrown.getMessage().contains("1536 MiB"), thrown.getMessage());
  }

  /**
   * The knobs the shipped manifest sets must fit the same container, or the refusal has no remedy.
   */
  @Test
  void theCorrectedManifestArithmeticFits() {
    BuildConfig config = knobs(4, 32L << 20, 614L << 20, 256L << 20);
    assertDoesNotThrow(() -> BuildBudget.check(config, 1536L << 20));
  }

  /**
   * Term arenas are counted because they are ALLOCATED, not grown into — the one term in the sum
   * that is certain rather than a ceiling.
   *
   * <p>Verified by breaking it: dropping the arena term from {@code requiredBytes} makes this pass
   * when it should not, because the caches alone fit comfortably.
   */
  @Test
  void arenasAloneCanExhaustTheHeap() {
    // tiny caches and no gram layer: only the arenas are large
    BuildConfig config =
        io.kahshe.common.Records.with(
            knobs(16, 128L << 20, 8L << 20, 0), java.util.Map.of("gramIndexEnabled", false));
    assertThrows(IllegalStateException.class, () -> BuildBudget.check(config, 1024L << 20));
    assertTrue(
        BuildBudget.requiredBytes(config) >= (16L * 128L) << 20,
        "the arenas the build allocates up front are not in the total");
  }

  /** A disabled tier costs nothing, so turning one off is a real remedy rather than cosmetic. */
  @Test
  void disabledTiersAreNotCharged() {
    BuildConfig both = knobs(8, 64L << 20, 64L << 20, 512L << 20);
    BuildConfig neither =
        io.kahshe.common.Records.with(
            both, java.util.Map.of("gramIndexEnabled", false, "termIndexEnabled", false));
    assertTrue(
        BuildBudget.requiredBytes(neither) < BuildBudget.requiredBytes(both),
        "disabling the gram and term tiers did not reduce the committed total, so the advice to "
            + "turn them off would not actually help an operator who is out of heap");
  }
}
