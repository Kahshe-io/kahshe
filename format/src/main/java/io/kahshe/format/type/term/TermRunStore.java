package io.kahshe.format.type.term;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One build's local scratch: the directory the runs live in, the allocator that names them, and the
 * only shared state the term path has.
 *
 * <p>Every run name comes from here rather than from each reader, and runs open {@code CREATE_NEW}
 * so a collision fails the build. A locally chosen name collides across readers, and
 * {@code Files.newOutputStream} defaults to CREATE plus TRUNCATE_EXISTING — a silent overwrite of a
 * finished run means terms gone, index published, and files pruned that match.
 *
 * <p>The directory carries a per-build nonce, so a retried build cannot find the previous build's
 * runs, and it is swept on close on the failure path as well as the success one — the merge consumes
 * the list of runs the readers returned, never a directory listing.
 * {@code KAHSHE_TERM_BUILD_MAX_SPILL_BYTES} is enforced here rather than inside the buffer, because
 * the volume is shared across readers, and by {@link Files#size} rather than by an estimate.
 */
public final class TermRunStore implements Closeable, RunBuffer.Names, RunMerger.Names {
  private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(TermRunStore.class);

  /** Reader slots are 0..threads; intermediates start well past any plausible reader count. */
  private static final int INTERMEDIATE_SLOT_BASE = 1_000_000;

  private final Path buildDir;
  private final long maxBytes;
  private final io.kahshe.common.Metrics metrics;
  private final AtomicLong written = new AtomicLong();
  private final AtomicLong runs = new AtomicLong();

  private Path root;

  public TermRunStore(String buildDir, long maxSpillBytes, io.kahshe.common.Metrics metrics) {
    this.buildDir = Path.of(buildDir);
    this.maxBytes = Math.max(1, maxSpillBytes);
    this.metrics = metrics;
  }

  /**
   * Fails now if the scratch directory cannot be written, rather than after a full pass over the
   * corpus: every build writes runs, and discovering it on the first flush wastes every file read
   * so far.
   */
  public void probe() throws IOException {
    Files.createDirectories(buildDir);
    Path check = Files.createTempFile(buildDir, "probe-", ".tmp");
    Files.deleteIfExists(check);
  }

  /**
   * Names an intermediate run for a cascading merge level.
   *
   * <p>The slot is offset past any reader slot so an intermediate can never collide with a run a
   * reader wrote — both are {@code CREATE_NEW}, so a collision fails the build rather than
   * corrupting anything.
   */
  @Override
  public Path next(int level, int sequence, boolean intermediate) throws IOException {
    metrics.termBuildRunMerges.increment();
    return next(INTERMEDIATE_SLOT_BASE + level, sequence);
  }

  /**
   * Releases a run the cascade is about to delete.
   *
   * <p>The budget tracks what is on disk, not everything ever written. Without this a cascading
   * merge charges for both the runs it reads and the run it writes, and a build could fail a
   * budget it was never actually over.
   */
  @Override
  public void released(Path run) throws IOException {
    if (Files.exists(run)) {
      written.addAndGet(-Files.size(run));
    }
  }

  @Override
  public synchronized Path next(int readerSlot, int sequence) throws IOException {
    if (root == null) {
      Files.createDirectories(buildDir);
      root = Files.createTempDirectory(buildDir, "term-build-");
    }
    runs.incrementAndGet();
    return root.resolve("run-" + readerSlot + "-" + sequence + ".terms");
  }

  /**
   * Records a finished run's size and fails the build if the scratch budget is spent.
   *
   * <p>Called after the run is closed, so the number is the file's real size rather than a guess at
   * what it will be.
   */
  @Override
  public void finished(Path run) throws IOException {
    metrics.termBuildSpills.increment();
    long total = written.addAndGet(Files.size(run));
    if (total > maxBytes) {
      throw new IOException(
          "term index build needs more than KAHSHE_TERM_BUILD_MAX_SPILL_BYTES="
              + maxBytes
              + " of local scratch (sorted runs now hold "
              + total
              + " bytes under "
              + buildDir
              + "). Raise the budget, give the build a larger volume, or narrow"
              + " kahshe.index.scope so it covers fewer data files.");
    }
  }

  public long bytesWritten() {
    return written.get();
  }

  public long runsWritten() {
    return runs.get();
  }

  private final java.util.concurrent.ConcurrentHashMap<Path, long[]> bounds =
      new java.util.concurrent.ConcurrentHashMap<>();

  @Override
  public void offsets(Path run, long[] rangeBounds) {
    bounds.put(run, rangeBounds);
  }

  @Override
  public long[] offsets(Path run) {
    return bounds.get(run);
  }

  /** Deletes this build's runs. Safe to call twice, and called on the failure path too. */
  @Override
  public void close() {
    if (root == null) {
      return;
    }
    Path doomed = root;
    root = null;
    try (var stream = Files.walk(doomed)) {
      stream
          .sorted(Comparator.reverseOrder())
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (IOException ignored) {
                  // best effort; the next build's directory carries a fresh nonce
                }
              });
    } catch (IOException ignored) {
      // best effort
    }
    LOG.info("swept term run scratch under {}", doomed);
  }
}
