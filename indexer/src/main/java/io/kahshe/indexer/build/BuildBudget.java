package io.kahshe.indexer.build;

import io.kahshe.indexer.BuildConfig;

/**
 * Refuses a build, before its first data file is opened, whose memory knobs cannot fit the heap
 * they are running in. Refusing is safe: an unindexed table is correct and merely unpruned.
 *
 * <p>It adds up only numbers an operator configured — the serving caches, {@code threads x
 * termBufferBytes} of term arenas, and the gram build ceiling counted as if it were already spent —
 * and never estimates the size of a growable structure, so the sum is exact and the refusal can
 * name both numbers and what to change. An arithmetically impossible configuration is invisible at
 * test scale, where no budget is ever exercised, and only shows itself hours into a real build.
 */
public class BuildBudget {

  /**
   * Fraction of the heap the configured knobs may claim. The remainder is not slack: a build also
   * holds Parquet buffers, the bloom map for every file it has read, roaring bitmaps mid-merge and,
   * in the default {@code KAHSHE_MODE=both}, the serving path. None of those are configured, so
   * none can be added up here; a fifth of the heap for them is a deliberately loose judgement,
   * because this check catches impossible configurations rather than tuning tight ones.
   */
  private static final double HEAP_FRACTION = 0.8;

  private BuildBudget() {}

  /** Bytes the configuration commits the build to, before a single data file is opened. */
  static long requiredBytes(BuildConfig config) {
    long caches =
        config.format().indexCacheBytes()
            + config.format().termCacheBytes()
            + config.format().planCacheBytes()
            + (config.format().gramIndexEnabled() ? config.format().gramCacheBytes() : 0);
    long arenas =
        config.format().termIndexEnabled()
            ? (long) Math.max(1, config.indexThreads()) * config.termBufferBytes()
            : 0;
    long gramCap = config.format().gramIndexEnabled() ? config.gramBuildMaxBytes() : 0;
    return caches + arenas + gramCap;
  }

  /**
   * Throws when the knobs cannot fit {@code maxHeapBytes}.
   *
   * @param maxHeapBytes taken from the runtime by the caller, so a test can supply its own
   */
  static void check(BuildConfig config, long maxHeapBytes) {
    long required = requiredBytes(config);
    long allowed = (long) (maxHeapBytes * HEAP_FRACTION);
    if (required <= allowed) {
      return;
    }
    long arenas =
        config.format().termIndexEnabled()
            ? (long) Math.max(1, config.indexThreads()) * config.termBufferBytes()
            : 0;
    throw new IllegalStateException(
        String.format(
            "index build refused: its knobs commit %d MiB but the heap is %d MiB (usable %d MiB). "
                + "caches=%d MiB, term arenas=%d MiB (KAHSHE_INDEX_THREADS=%d x "
                + "KAHSHE_TERM_BUFFER_BYTES=%d MiB), gram cap=%d MiB "
                + "(KAHSHE_GRAM_BUILD_MAX_BYTES). Raise the container's memory limit, or lower "
                + "KAHSHE_INDEX_THREADS / KAHSHE_TERM_BUFFER_BYTES / KAHSHE_GRAM_BUILD_MAX_BYTES / "
                + "KAHSHE_CACHE_BYTES. Refusing now beats an OOM part-way through a build: the "
                + "table is simply unpruned until this is fixed, which is correct.",
            mib(required), mib(maxHeapBytes), mib(allowed),
            mib(required - arenas - (config.format().gramIndexEnabled() ? config.gramBuildMaxBytes() : 0)),
            mib(arenas),
            Math.max(1, config.indexThreads()),
            mib(config.termBufferBytes()),
            mib(config.format().gramIndexEnabled() ? config.gramBuildMaxBytes() : 0)));
  }

  private static long mib(long bytes) {
    return bytes / (1024 * 1024);
  }
}
